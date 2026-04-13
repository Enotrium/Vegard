"""Transport bus abstraction - gRPC + Protobuf / MQTT

gRPC: drone↔node (binary streaming, low-latency)
MQTT: cloud↔AIP (async pub/sub, familiar HTTP/JSON)
"""

import asyncio
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional

import structlog
from paho.mqtt import client as mqtt

logger = structlog.get_logger()


@dataclass
class TransportConfig:
    """Transport configuration"""

    # gRPC settings
    grpc_port: int = 50051
    grpc_max_workers: int = 10

    # MQTT settings
    mqtt_broker: str = "localhost"
    mqtt_port: int = 1883
    mqtt_username: Optional[str] = None
    mqtt_password: Optional[str] = None
    mqtt_client_id: str = "syndar-node"
    mqtt_qos: int = 1

    # Protocol selection
    use_grpc: bool = True
    use_mqtt: bool = True


class TransportBus:
    """Multi-protocol transport abstraction"""

    def __init__(self, config: Optional[TransportConfig] = None):
        self.config = config or TransportConfig()
        self._mqtt_client: Optional[mqtt.Client] = None
        self._grpc_server: Optional[Any] = None
        self._handlers: dict[str, list[Callable]] = {}
        self._subscribed_topics: set[str] = set()
        self._lock = asyncio.Lock()
        self._running = False

    async def start(self) -> None:
        """Start transport services"""
        self._running = True

        if self.config.use_mqtt:
            await self._start_mqtt()

        if self.config.use_grpc:
            await self._start_grpc()

        logger.info(
            "Transport bus started",
            grpc=self.config.use_grpc,
            mqtt=self.config.use_mqtt,
        )

    async def stop(self) -> None:
        """Stop transport services"""
        self._running = False

        if self._mqtt_client:
            self._mqtt_client.disconnect()
            logger.info("MQTT disconnected")

        if self._grpc_server:
            self._grpc_server.stop(5)
            logger.info("gRPC server stopped")

    async def _start_mqtt(self) -> None:
        """Start MQTT client"""
        self._mqtt_client = mqtt.Client(client_id=self.config.mqtt_client_id)

        if self.config.mqtt_username:
            self._mqtt_client.username_pw_set(
                self.config.mqtt_username, self.config.mqtt_password
            )

        self._mqtt_client.on_connect = self._on_mqtt_connect
        self._mqtt_client.on_message = self._on_mqtt_message
        self._mqtt_client.on_disconnect = self._on_mqtt_disconnect

        try:
            self._mqtt_client.connect(
                self.config.mqtt_broker, self.config.mqtt_port, keepalive=60
            )
            # Run in background thread
            self._mqtt_client.loop_start()
            logger.info(
                "MQTT connected",
                broker=self.config.mqtt_broker,
                port=self.config.mqtt_port,
            )
        except Exception as e:
            logger.error("MQTT connection failed", error=str(e))
            raise

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        """MQTT connect callback"""
        if rc == 0:
            logger.info("MQTT connected successfully")
            # Resubscribe to topics
            for topic in self._subscribed_topics:
                client.subscribe(topic, qos=self.config.mqtt_qos)
        else:
            logger.error("MQTT connection failed", code=rc)

    def _on_mqtt_disconnect(self, client, userdata, rc):
        """MQTT disconnect callback"""
        if rc != 0:
            logger.warning("MQTT unexpected disconnection", code=rc)

    def _on_mqtt_message(self, client, userdata, msg):
        """MQTT message callback"""
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
            self._dispatch(msg.topic, payload)
        except json.JSONDecodeError:
            logger.warning("Invalid JSON in MQTT message", topic=msg.topic)
        except Exception as e:
            logger.exception("MQTT message handler error", topic=msg.topic)

    async def _start_grpc(self) -> None:
        """Start gRPC server (async - for future implementation)"""
        # gRPC server will be implemented when proto compilation is set up
        logger.info("gRPC server ready (proto compilation pending)")

    async def publish(
        self, topic: str, payload: dict, protocol: Optional[str] = None
    ) -> None:
        """Publish message to topic"""
        # Auto-select protocol based on topic
        if protocol is None:
            if topic.startswith("drone/") or topic.startswith("mesh/"):
                protocol = "grpc"
            else:
                protocol = "mqtt"

        if protocol == "mqtt" and self._mqtt_client:
            await self._publish_mqtt(topic, payload)
        elif protocol == "grpc":
            await self._publish_grpc(topic, payload)
        else:
            logger.warning("No transport available for protocol", protocol=protocol)

    async def _publish_mqtt(self, topic: str, payload: dict) -> None:
        """Publish via MQTT"""
        if not self._mqtt_client:
            return

        message = json.dumps(payload, default=str)
        info = self._mqtt_client.publish(topic, message, qos=self.config.mqtt_qos)

        if info.rc == mqtt.MQTT_ERR_SUCCESS:
            logger.debug("Published to MQTT", topic=topic)
        else:
            logger.warning("MQTT publish failed", topic=topic, code=info.rc)

    async def _publish_grpc(self, topic: str, payload: dict) -> None:
        """Publish via gRPC (stub for now)"""
        # TODO: Implement gRPC streaming
        logger.debug("gRPC publish (stub)", topic=topic)

    def subscribe(self, topic: str, handler: Callable[[dict], None]) -> None:
        """Subscribe to topic"""
        if topic not in self._handlers:
            self._handlers[topic] = []
            self._subscribed_topics.add(topic)

            # MQTT subscribe if connected
            if self._mqtt_client and self._mqtt_client.is_connected():
                self._mqtt_client.subscribe(topic, qos=self.config.mqtt_qos)

        self._handlers[topic].append(handler)
        logger.debug("Subscribed to topic", topic=topic)

    def unsubscribe(self, topic: str, handler: Callable[[dict], None]) -> None:
        """Unsubscribe from topic"""
        if topic in self._handlers and handler in self._handlers[topic]:
            self._handlers[topic].remove(handler)
            if not self._handlers[topic]:
                del self._handlers[topic]
                self._subscribed_topics.discard(topic)

                # MQTT unsubscribe
                if self._mqtt_client and self._mqtt_client.is_connected():
                    self._mqtt_client.unsubscribe(topic)

        logger.debug("Unsubscribed from topic", topic=topic)

    def _dispatch(self, topic: str, payload: dict) -> None:
        """Dispatch message to handlers"""
        # Direct match
        if topic in self._handlers:
            for handler in self._handlers[topic]:
                try:
                    handler(payload)
                except Exception:
                    logger.exception("Handler failed", topic=topic)

        # Wildcard match (topic/#)
        for subscribed_topic, handlers in self._handlers.items():
            if subscribed_topic.endswith("/#"):
                prefix = subscribed_topic[:-2]
                if topic.startswith(prefix):
                    for handler in handlers:
                        try:
                            handler(payload)
                        except Exception:
                            logger.exception("Handler failed", topic=topic)

    async def rpc_call(
        self, target: str, method: str, payload: dict, timeout: float = 30.0
    ) -> dict:
        """Make RPC call to target"""
        # TODO: Implement gRPC unary calls
        logger.debug("RPC call (stub)", target=target, method=method)
        return {"status": "stub", "target": target, "method": method}

    def is_connected(self) -> bool:
        """Check if transport is connected"""
        if self._mqtt_client:
            return self._mqtt_client.is_connected()
        return False
