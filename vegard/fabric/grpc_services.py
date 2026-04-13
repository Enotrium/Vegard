"""gRPC service implementations for Vegard

Implements MeshService, TaskService, DriftService, and AIPBridgeService
using the compiled proto stubs.
"""

import asyncio
import json
from typing import AsyncIterator

import grpc
import structlog

from syndar.exceptions import TransportError
from syndar.proto import transport_pb2, transport_pb2_grpc

logger = structlog.get_logger()


class MeshService(transport_pb2_grpc.MeshServiceServicer):
    """gRPC MeshService implementation for entity state streaming"""

    def __init__(self, mesh):
        self.mesh = mesh
        self._entity_queues: dict[str, asyncio.Queue] = {}

    async def StreamEntities(
        self, request_iterator: AsyncIterator[transport_pb2.BytesMessage], context: grpc.ServicerContext
    ) -> AsyncIterator[transport_pb2.BytesMessage]:
        """Bidirectional streaming for real-time entity state"""
        try:
            async for request in request_iterator:
                # Deserialize incoming entity
                try:
                    entity_data = json.loads(request.data.decode("utf-8"))
                    entity_id = entity_data.get("entity_id")
                    
                    if entity_id:
                        # Create queue for this stream if needed
                        if entity_id not in self._entity_queues:
                            self._entity_queues[entity_id] = asyncio.Queue(maxsize=100)
                        
                        # Process incoming entity update
                        await self._process_entity_update(entity_data)
                        
                        # Send back entity updates for this stream
                        if not self._entity_queues[entity_id].empty():
                            update = self._entity_queues[entity_id].get_nowait()
                            yield update
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON in StreamEntities")
                except Exception as e:
                    logger.error("Error in StreamEntities", error=str(e))
        except Exception as e:
            logger.error("StreamEntities failed", error=str(e))
            raise TransportError(f"StreamEntities failed: {str(e)}") from e

    async def PublishEntity(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.PublishAck:
        """Unary entity update"""
        try:
            entity_data = json.loads(request.data.decode("utf-8"))
            entity_id = entity_data.get("entity_id")
            
            await self._process_entity_update(entity_data)
            
            return transport_pb2.PublishAck(
                entity_id=entity_id,
                received_at_ms=int(asyncio.get_event_loop().time() * 1000),
                valid=True,
                error="",
            )
        except Exception as e:
            logger.error("PublishEntity failed", error=str(e))
            return transport_pb2.PublishAck(
                entity_id="",
                received_at_ms=int(asyncio.get_event_loop().time() * 1000),
                valid=False,
                error=str(e),
            )

    async def SyncEntities(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.SyncAck:
        """Batch entity sync"""
        try:
            entity_data = json.loads(request.data.decode("utf-8"))
            entities = entity_data.get("entities", [])
            
            for entity in entities:
                await self._process_entity_update(entity)
            
            return transport_pb2.SyncAck(
                received_count=len(entities),
                received_at_ms=int(asyncio.get_event_loop().time() * 1000),
            )
        except Exception as e:
            logger.error("SyncEntities failed", error=str(e))
            raise TransportError(f"SyncEntities failed: {str(e)}") from e

    async def QueryEntities(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.EntityHistory:
        """Query entity history"""
        try:
            query_data = json.loads(request.data.decode("utf-8"))
            entity_id = query_data.get("entity_id")
            
            # Get entity history from mesh
            if self.mesh:
                from syndar.fabric.mesh import EntityState
                
                history = await self.mesh.store.get_history(
                    entity_id, 
                    query_data.get("start_time_ms", 0),
                    query_data.get("end_time_ms", 0)
                )
                
                states = []
                for entity in history:
                    states.append(json.dumps(entity.model_dump(), default=str).encode("utf-8"))
                
                return transport_pb2.EntityHistory(
                    entity_id=entity_id,
                    states=states,
                )
            else:
                return transport_pb2.EntityHistory(entity_id=entity_id)
        except Exception as e:
            logger.error("QueryEntities failed", error=str(e))
            raise TransportError(f"QueryEntities failed: {str(e)}") from e

    async def Heartbeat(
        self, request: transport_pb2.HeartbeatRequest, context: grpc.ServicerContext
    ) -> transport_pb2.HeartbeatResponse:
        """Heartbeat for liveness"""
        return transport_pb2.HeartbeatResponse(
            entity_id=request.entity_id,
            server_time_ms=int(asyncio.get_event_loop().time() * 1000),
            status=transport_pb2.MeshStatus.MESH_STATUS_HEALTHY,
        )

    async def _process_entity_update(self, entity_data: dict) -> None:
        """Process incoming entity update"""
        if not self.mesh:
            return
        
        from syndar.fabric.mesh import EntityState, Position
        
        # Convert dict to EntityState
        entity = EntityState(
            entity_id=entity_data.get("entity_id", ""),
            entity_type=entity_data.get("entity_type", "drone"),
            position=Position(
                lat=entity_data.get("position", {}).get("lat", 0.0),
                lng=entity_data.get("position", {}).get("lng", 0.0),
                alt=entity_data.get("position", {}).get("alt", 0.0),
            ),
            timestamp_ms=entity_data.get("timestamp_ms", 0),
            drift_score=entity_data.get("drift_score", 0.0),
            drift_flag=entity_data.get("drift_flag", False),
            battery_pct=entity_data.get("battery_pct", 100.0),
            task_id=entity_data.get("task_id", ""),
        )
        
        await self.mesh.store.update(entity)
        
        # Add to streaming queues
        for entity_id, queue in self._entity_queues.items():
            try:
                serialized = json.dumps(entity.model_dump(), default=str).encode("utf-8")
                msg = transport_pb2.BytesMessage(data=serialized, content_type="application/json")
                queue.put_nowait(msg)
            except asyncio.QueueFull:
                pass


class TaskService(transport_pb2_grpc.TaskServiceServicer):
    """gRPC TaskService implementation for task auction and management"""

    def __init__(self, task_allocator):
        self.task_allocator = task_allocator

    async def PublishTask(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.TaskPublished:
        """Publish a new task for bidding"""
        try:
            task_data = json.loads(request.data.decode("utf-8"))
            
            from syndar.fabric.task_allocator import TaskRequest
            task = TaskRequest(**task_data)
            
            await self.task_allocator.publish_task(task)
            
            return transport_pb2.TaskPublished(
                task_id=task.task_id,
                published_at_ms=int(asyncio.get_event_loop().time() * 1000),
                subscriber_count=0,  # TODO: track subscribers
            )
        except Exception as e:
            logger.error("PublishTask failed", error=str(e))
            raise TransportError(f"PublishTask failed: {str(e)}") from e

    async def Bid(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.BidAck:
        """Submit a bid"""
        try:
            bid_data = json.loads(request.data.decode("utf-8"))
            
            from syndar.fabric.task_allocator import TaskBid
            bid = TaskBid(**bid_data)
            
            await self.task_allocator.submit_bid(bid)
            
            return transport_pb2.BidAck(
                task_id=bid.task_id,
                entity_id=bid.entity_id,
                accepted=True,  # TODO: check auction status
                rejection_reason="",
            )
        except Exception as e:
            logger.error("Bid failed", error=str(e))
            raise TransportError(f"Bid failed: {str(e)}") from e

    async def AcceptTask(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.TaskAssigned:
        """Accept/assign a task"""
        try:
            ack_data = json.loads(request.data.decode("utf-8"))
            
            return transport_pb2.TaskAssigned(
                task_id=ack_data.get("task_id", ""),
                entity_id=ack_data.get("entity_id", ""),
                assigned_at_ms=int(asyncio.get_event_loop().time() * 1000),
                deadline_ms=ack_data.get("deadline_ms", 0),
            )
        except Exception as e:
            logger.error("AcceptTask failed", error=str(e))
            raise TransportError(f"AcceptTask failed: {str(e)}") from e

    async def StreamProgress(
        self, request_iterator: AsyncIterator[transport_pb2.BytesMessage], context: grpc.ServicerContext
    ) -> AsyncIterator[transport_pb2.BytesMessage]:
        """Stream progress updates"""
        try:
            async for request in request_iterator:
                # Process progress updates
                progress_data = json.loads(request.data.decode("utf-8"))
                
                # Echo back for now
                yield request
        except Exception as e:
            logger.error("StreamProgress failed", error=str(e))
            raise TransportError(f"StreamProgress failed: {str(e)}") from e

    async def CompleteTask(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.CompletionAck:
        """Report task completion"""
        try:
            result_data = json.loads(request.data.decode("utf-8"))
            
            from syndar.fabric.task_allocator import TaskResult
            result = TaskResult(**result_data)
            
            await self.task_allocator.complete_task(result)
            
            return transport_pb2.CompletionAck(
                task_id=result.task_id,
                accepted=True,
                rejection_reason="",
                processed_at_ms=int(asyncio.get_event_loop().time() * 1000),
            )
        except Exception as e:
            logger.error("CompleteTask failed", error=str(e))
            raise TransportError(f"CompleteTask failed: {str(e)}") from e

    async def CancelTask(
        self, request: transport_pb2.CancelRequest, context: grpc.ServicerContext
    ) -> transport_pb2.CancelResponse:
        """Cancel a task"""
        try:
            # TODO: Implement task cancellation
            return transport_pb2.CancelResponse(
                task_id=request.task_id,
                cancelled=True,
                status="cancelled",
            )
        except Exception as e:
            logger.error("CancelTask failed", error=str(e))
            raise TransportError(f"CancelTask failed: {str(e)}") from e


class DriftService(transport_pb2_grpc.DriftServiceServicer):
    """gRPC DriftService implementation for cross-node drift monitoring"""

    def __init__(self, drift_monitor):
        self.drift_monitor = drift_monitor

    async def StreamDrift(
        self, request_iterator: AsyncIterator[transport_pb2.BytesMessage], context: grpc.ServicerContext
    ) -> AsyncIterator[transport_pb2.BytesMessage]:
        """Stream drift signals from nodes"""
        try:
            async for request in request_iterator:
                # Process drift signal
                signal_data = json.loads(request.data.decode("utf-8"))
                
                from syndar.fabric.drift_monitor import NodeDriftSignal
                signal = NodeDriftSignal(**signal_data)
                
                await self.drift_monitor.report_signal(signal)
                
                # Check for alerts
                alerts = await self.drift_monitor.get_alerts()
                if alerts:
                    for alert in alerts:
                        alert_data = json.dumps(alert.model_dump(), default=str).encode("utf-8")
                        yield transport_pb2.BytesMessage(data=alert_data, content_type="application/json")
        except Exception as e:
            logger.error("StreamDrift failed", error=str(e))
            raise TransportError(f"StreamDrift failed: {str(e)}") from e

    async def QueryDrift(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.BytesMessage:
        """Query drift correlations"""
        try:
            query_data = json.loads(request.data.decode("utf-8"))
            field_id = query_data.get("field_id")
            
            correlations = await self.drift_monitor.get_correlations(field_id)
            report_data = json.dumps([c.model_dump() for c in correlations], default=str).encode("utf-8")
            
            return transport_pb2.BytesMessage(data=report_data, content_type="application/json")
        except Exception as e:
            logger.error("QueryDrift failed", error=str(e))
            raise TransportError(f"QueryDrift failed: {str(e)}") from e

    async def SubscribeAlerts(
        self, request: transport_pb2.AlertSubscription, context: grpc.ServicerContext
    ) -> AsyncIterator[transport_pb2.BytesMessage]:
        """Subscribe to drift alerts"""
        try:
            # TODO: Implement alert subscription
            while True:
                await asyncio.sleep(1)
                # Yield alerts when they occur
        except Exception as e:
            logger.error("SubscribeAlerts failed", error=str(e))
            raise TransportError(f"SubscribeAlerts failed: {str(e)}") from e

    async def RequestRecalibration(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.BytesMessage:
        """Request model recalibration"""
        try:
            # TODO: Implement recalibration request
            response_data = json.dumps({"status": "requested"}).encode("utf-8")
            return transport_pb2.BytesMessage(data=response_data, content_type="application/json")
        except Exception as e:
            logger.error("RequestRecalibration failed", error=str(e))
            raise TransportError(f"RequestRecalibration failed: {str(e)}") from e


class AIPBridgeService(transport_pb2_grpc.AIPBridgeServiceServicer):
    """gRPC AIPBridgeService implementation for AIP integration"""

    def __init__(self, aip_bridge):
        self.aip_bridge = aip_bridge

    async def IngestSoil(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.IngestAck:
        """Ingest soil predictions"""
        try:
            soil_data = json.loads(request.data.decode("utf-8"))
            
            # TODO: Implement actual ingestion via AIP bridge
            return transport_pb2.IngestAck(
                scan_id=soil_data.get("scan_id", ""),
                accepted=True,
                aip_record_id=f"aip-{int(asyncio.get_event_loop().time() * 1000)}",
                processed_at_ms=int(asyncio.get_event_loop().time() * 1000),
                error="",
            )
        except Exception as e:
            logger.error("IngestSoil failed", error=str(e))
            raise TransportError(f"IngestSoil failed: {str(e)}") from e

    async def IngestBatch(
        self, request: transport_pb2.BytesMessage, context: grpc.ServicerContext
    ) -> transport_pb2.BatchIngestAck:
        """Batch ingest"""
        try:
            batch_data = json.loads(request.data.decode("utf-8"))
            predictions = batch_data.get("predictions", [])
            
            # TODO: Implement batch ingestion
            return transport_pb2.BatchIngestAck(
                field_id=batch_data.get("field_id", ""),
                accepted_count=len(predictions),
                rejected_count=0,
                processed_at_ms=int(asyncio.get_event_loop().time() * 1000),
            )
        except Exception as e:
            logger.error("IngestBatch failed", error=str(e))
            raise TransportError(f"IngestBatch failed: {str(e)}") from e

    async def QueryFieldStatus(
        self, request: transport_pb2.FieldStatusRequest, context: grpc.ServicerContext
    ) -> transport_pb2.FieldStatusResponse:
        """Query field status from AIP perspective"""
        try:
            # TODO: Implement field status query
            return transport_pb2.FieldStatusResponse(
                field_id=request.field_id,
                exists=True,
                scan_coverage_pct=0.0,
                prediction_count=0,
                last_scan_at_ms=0,
                aip_contract_status="pending",
            )
        except Exception as e:
            logger.error("QueryFieldStatus failed", error=str(e))
            raise TransportError(f"QueryFieldStatus failed: {str(e)}") from e


def register_services(server, mesh=None, task_allocator=None, drift_monitor=None, aip_bridge=None):
    """Register all gRPC services with the server"""
    transport_pb2_grpc.add_MeshServiceServicer_to_server(
        MeshService(mesh), server
    )
    transport_pb2_grpc.add_TaskServiceServicer_to_server(
        TaskService(task_allocator), server
    )
    transport_pb2_grpc.add_DriftServiceServicer_to_server(
        DriftService(drift_monitor), server
    )
    transport_pb2_grpc.add_AIPBridgeServiceServicer_to_server(
        AIPBridgeService(aip_bridge), server
    )
    
    logger.info("gRPC services registered")
