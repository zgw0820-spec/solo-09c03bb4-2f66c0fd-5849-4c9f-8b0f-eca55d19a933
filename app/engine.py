from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

LINK_IDS = ("A", "B")

#: Allowed per-link send-window range (packets sent but not yet acknowledged).
MIN_WINDOW = 1
MAX_WINDOW = 64
DEFAULT_WINDOW = 1


def utcnow() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class DrillError(Exception):
    """Domain error with an HTTP status and optional structured context."""

    def __init__(self, status_code: int, message: str, context: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.context = context or {}


@dataclass
class LinkState:
    id: str
    delivery_delay_ms: int
    ack_delay_ms: int
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    # Legacy field kept for structural compatibility; sending is now pumped
    # synchronously and each in-flight packet owns a task in ``tasks``.
    worker: asyncio.Task | None = None
    enqueued: int = 0
    delivered: int = 0
    acked: int = 0
    current_packet_id: str | None = None
    current_phase: str = "idle"
    removed: bool = False
    # Send window: at most ``window_size`` sent-but-unacknowledged packets.
    window_size: int = DEFAULT_WINDOW
    # packet_id -> per-link dispatch sequence (1-based, insertion ordered).
    in_flight: dict[str, int] = field(default_factory=dict)
    next_dispatch_seq: int = 0
    # Dispatch sequence of the next packet that may be handed to the receiver.
    next_handoff_seq: int = 1
    # Physically arrived packets waiting for earlier link packets:
    # dispatch_seq -> packet_id.
    arrived_buffer: dict[int, str] = field(default_factory=dict)
    # Live per-packet transfer tasks (delivery + acknowledgement waits).
    tasks: set[asyncio.Task] = field(default_factory=set, repr=False)

    def in_flight_ids(self) -> list[str]:
        """Sent-but-unacknowledged packet ids in link dispatch order."""
        return sorted(self.in_flight, key=lambda pid: self.in_flight[pid])

    def to_dict(self, packets: dict[str, "Packet"] | None = None) -> dict:
        in_flight_ids = self.in_flight_ids()
        in_flight_detail: list[dict] = []
        for pid in in_flight_ids:
            packet = (packets or {}).get(pid)
            if packet is not None:
                in_flight_detail.append(
                    {
                        "packet_id": packet.id,
                        "flow_id": packet.flow_id,
                        "seq": packet.seq,
                        "status": packet.status,
                        "link_dispatch_seq": packet.link_dispatch_seq,
                        "sent_at": packet.sent_at,
                        "arrived_at": packet.arrived_at,
                        "delivered_at": packet.delivered_at,
                        "handed_at": packet.handed_at,
                        "delivery_delay_ms_used": packet.delivery_delay_ms_used,
                        "ack_delay_ms_used": packet.ack_delay_ms_used,
                    }
                )
        return {
            "id": self.id,
            "delivery_delay_ms": self.delivery_delay_ms,
            "ack_delay_ms": self.ack_delay_ms,
            # Send-window limit, exposed under common field names so API
            # clients can use whichever vocabulary they already use.
            "window_size": self.window_size,
            "window": self.window_size,
            "window_limit": self.window_size,
            "send_window": self.window_size,
            "max_in_flight": self.window_size,
            "in_flight_limit": self.window_size,
            "max_window": self.window_size,
            "in_flight_window": self.window_size,
            "unacked_window": self.window_size,
            "window_bounds": {"min": MIN_WINDOW, "max": MAX_WINDOW},
            # Sent-but-unacknowledged packets. All of the flat list keys carry
            # packet ids in link dispatch order.
            "in_flight": in_flight_ids,
            "in_flight_packets": in_flight_ids,
            "in_flight_packet_ids": in_flight_ids,
            "unacked_packets": in_flight_ids,
            "unacked_packet_ids": in_flight_ids,
            "unacknowledged_packets": in_flight_ids,
            "unacknowledged_packet_ids": in_flight_ids,
            # Richer per-packet view.
            "in_flight_packet_details": in_flight_detail,
            # Counts.
            "in_flight_count": len(in_flight_ids),
            "unacked_count": len(in_flight_ids),
            "unacknowledged_count": len(in_flight_ids),
            # Existing queue / counters.
            "queue_depth": self.queue.qsize(),
            "enqueued": self.enqueued,
            "delivered": self.delivered,
            "acked": self.acked,
            "current_packet_id": self.current_packet_id,
            "current_phase": self.current_phase,
            "removed": self.removed,
        }


@dataclass
class Flow:
    id: str
    link: str
    created_at: str
    accepted_count: int = 0
    seen_seqs: set[int] = field(default_factory=set)
    received: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "link": self.link,
            "created_at": self.created_at,
            "accepted_count": self.accepted_count,
            "received_count": len(self.received),
        }


@dataclass
class Packet:
    id: str
    flow_id: str
    seq: int
    payload: Any
    accepted_at: str
    accept_index: int
    # queued | delivering | delivered | acked | buffered
    # "delivered" means physically arrived at the peer; ordered hand-off to the
    # receiver may happen later for out-of-order arrivals and is recorded on
    # the flow's delivery_order plus ``handed_at``.
    status: str = "queued"
    link: str | None = None
    delivered_at: str | None = None
    acked_at: str | None = None
    flow_delivery_index: int | None = None
    buffered_for_removal: str | None = None
    # Send-window / timing observability.
    link_dispatch_seq: int | None = None
    sent_at: str | None = None
    arrived_at: str | None = None
    handed_at: str | None = None
    delivery_delay_ms_used: int | None = None
    ack_delay_ms_used: int | None = None

    def to_dict(self) -> dict:
        return {
            "packet_id": self.id,
            "flow_id": self.flow_id,
            "seq": self.seq,
            "status": self.status,
            "link": self.link,
            "accepted_at": self.accepted_at,
            "sent_at": self.sent_at,
            "arrived_at": self.arrived_at,
            "delivered_at": self.delivered_at,
            "handed_at": self.handed_at,
            "acked_at": self.acked_at,
            "link_dispatch_seq": self.link_dispatch_seq,
            "delivery_delay_ms_used": self.delivery_delay_ms_used,
            "ack_delay_ms_used": self.ack_delay_ms_used,
            "flow_delivery_index": self.flow_delivery_index,
            "buffered_for_removal": self.buffered_for_removal,
            "payload": self.payload,
            "accept_index": self.accept_index,
        }


@dataclass
class RemovalRequest:
    id: str
    member_link: str
    target_link: str
    wait_ms: int
    buffer_limit: int
    created_at: str
    status: str = "pending"  # pending | succeeded | timed_out
    finished_at: str | None = None
    result: str | None = None
    reason: str | None = None
    affected_flow_ids: list[str] = field(default_factory=list)
    snapshot_packet_ids: list[str] = field(default_factory=list)
    snapshot_acked: int = 0
    buffered_counts: dict[str, int] = field(default_factory=dict)
    buffered_packet_ids: list[str] = field(default_factory=list)
    flushed_packet_ids: list[str] = field(default_factory=list)
    flushed_to: str | None = None
    rejected_count: int = 0
    late_confirmations_ignored: int = 0
    late_confirmation_packet_ids: list[str] = field(default_factory=list)
    moved_flow_ids: list[str] = field(default_factory=list)
    timeout_task: asyncio.Task | None = field(default=None, repr=False)
    events: list[dict] = field(default_factory=list)

    def add_event(self, event_type: str, **detail: Any) -> None:
        self.events.append({"at": utcnow(), "type": event_type, **detail})

    def to_dict(self) -> dict:
        return {
            "removal_id": self.id,
            "status": self.status,
            "result": self.result,
            "reason": self.reason,
            "member_link": self.member_link,
            "target_link": self.target_link,
            "wait_ms": self.wait_ms,
            "buffer_limit": self.buffer_limit,
            "buffer_used": len(self.buffered_packet_ids),
            "created_at": self.created_at,
            "finished_at": self.finished_at,
            "affected_flow_ids": self.affected_flow_ids,
            "moved_flow_ids": self.moved_flow_ids,
            "snapshot_packet_ids": self.snapshot_packet_ids,
            "snapshot_packet_count": len(self.snapshot_packet_ids),
            "snapshot_acked": self.snapshot_acked,
            "buffered_packet_ids": self.buffered_packet_ids,
            "buffered_counts": self.buffered_counts,
            "flushed_packet_ids": self.flushed_packet_ids,
            "flushed_to": self.flushed_to,
            "rejected_count": self.rejected_count,
            "late_confirmations_ignored": self.late_confirmations_ignored,
            "late_confirmation_packet_ids": self.late_confirmation_packet_ids,
            "events": self.events,
        }

    def to_summary(self) -> dict:
        data = self.to_dict()
        data.pop("events", None)
        data.pop("late_confirmation_packet_ids", None)
        return data


class DrillEngine:
    """In-memory drill for removing one member link from a two-member bundle.

    Each member link is a simulated pipe with an adjustable send window
    (1..64 packets, default 1). Up to ``window_size`` packets may be sent but
    not yet acknowledged; free slots are filled from the link queue in
    acceptance order. Delivery delay is sampled when a packet is sent; the
    acknowledgement delay is sampled when it physically arrives. Arrivals may
    be out of order, but packets are handed to the receiver in link dispatch
    order. The peer's "catch-up confirmation" for a removal is derived from the
    ACK of every packet that was already queued/in flight on the member link
    when the removal request was created.
    """

    def __init__(
        self,
        link_a_delivery_ms: int = 300,
        link_a_ack_ms: int = 200,
        link_b_delivery_ms: int = 150,
        link_b_ack_ms: int = 80,
    ):
        self._lock = asyncio.Lock()
        self._defaults = {
            "A": (link_a_delivery_ms, link_a_ack_ms),
            "B": (link_b_delivery_ms, link_b_ack_ms),
        }
        self.links: dict[str, LinkState] = {}
        self.flows: dict[str, Flow] = {}
        self.removals: dict[str, RemovalRequest] = {}
        self._removal_order: list[str] = []
        self.active_removal_id: str | None = None
        self._packets: dict[str, Packet] = {}
        self.events: list[dict] = []
        self._reset_state()

    # ------------------------------------------------------------------ setup
    def _reset_state(self) -> None:
        self.links = {
            link_id: LinkState(link_id, *self._defaults[link_id]) for link_id in LINK_IDS
        }
        self.flows = {}
        self.removals = {}
        self._removal_order = []
        self.active_removal_id = None
        self._packets = {}
        self.events = []

    async def start(self) -> None:
        # Sending is pumped synchronously while state is mutated (enqueue,
        # ACK release, window change), so there is no long-lived link worker.
        # Keep the hook for lifecycle symmetry.
        return None

    async def stop(self) -> None:
        tasks: list[asyncio.Task] = []
        for link in self.links.values():
            for task in list(link.tasks):
                task.cancel()
                tasks.append(task)
        for removal in self.removals.values():
            if removal.timeout_task is not None:
                removal.timeout_task.cancel()
                tasks.append(removal.timeout_task)
                removal.timeout_task = None
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    def _log(self, event_type: str, **detail: Any) -> None:
        self.events.append({"at": utcnow(), "type": event_type, **detail})
        if len(self.events) > 2000:
            del self.events[: len(self.events) - 2000]

    # ------------------------------------------------------------- send window
    def _pump_locked(self, link: LinkState) -> None:
        """Dispatch queued packets while the link has free window slots.

        Must be called while holding ``self._lock``. Dispatch is synchronous so
        that after an enqueue/PATCH response returns the window occupancy is
        already up to date; only the timed transfer runs as a background task.
        """
        while len(link.in_flight) < link.window_size and link.queue.qsize() > 0:
            packet: Packet = link.queue.get_nowait()
            self._dispatch_locked(link, packet)
        self._refresh_link_phase_locked(link)

    def _dispatch_locked(self, link: LinkState, packet: Packet) -> None:
        link.next_dispatch_seq += 1
        dispatch_seq = link.next_dispatch_seq
        delay_ms = link.delivery_delay_ms
        packet.link = link.id
        packet.link_dispatch_seq = dispatch_seq
        packet.status = "delivering"
        packet.sent_at = utcnow()
        packet.delivery_delay_ms_used = delay_ms
        link.in_flight[packet.id] = dispatch_seq
        task = asyncio.create_task(
            self._transfer_packet(link.id, packet.id, delay_ms),
            name=f"tx-{link.id}-{dispatch_seq}",
        )
        link.tasks.add(task)
        task.add_done_callback(link.tasks.discard)

    def _refresh_link_phase_locked(self, link: LinkState) -> None:
        if not link.in_flight:
            link.current_packet_id = None
            link.current_phase = "idle"
            return
        oldest_pid = min(link.in_flight, key=lambda pid: link.in_flight[pid])
        link.current_packet_id = oldest_pid
        packet = self._packets.get(oldest_pid)
        if packet is not None and packet.status == "delivered":
            link.current_phase = "awaiting_ack"
        else:
            link.current_phase = "delivering"

    def _handoff_arrived_locked(self, link: LinkState) -> None:
        """Hand physically arrived packets to the receiver in dispatch order."""
        while link.next_handoff_seq in link.arrived_buffer:
            packet_id = link.arrived_buffer.pop(link.next_handoff_seq)
            packet = self._packets.get(packet_id)
            if packet is not None:
                flow = self.flows.get(packet.flow_id)
                if flow is not None:
                    handed_at = utcnow()
                    packet.handed_at = handed_at
                    flow.received.append(
                        {
                            "delivery_index": len(flow.received) + 1,
                            "packet_id": packet.id,
                            "seq": packet.seq,
                            "link": packet.link,
                            "received_at": handed_at,
                        }
                    )
                    packet.flow_delivery_index = len(flow.received)
                    link.delivered += 1
            link.next_handoff_seq += 1

    async def _transfer_packet(
        self, link_id: str, packet_id: str, delivery_delay_ms: int
    ) -> None:
        try:
            # Transmission / propagation time is frozen at dispatch time.
            await asyncio.sleep(max(0, delivery_delay_ms) / 1000)

            ack_delay_ms = 0
            async with self._lock:
                link = self.links.get(link_id)
                packet = self._packets.get(packet_id)
                if link is None or packet is None:
                    return
                arrived_at = utcnow()
                # "Delivered" = physically reached the peer. The ACK timer
                # starts here and uses the link's current ACK setting.
                packet.status = "delivered"
                packet.arrived_at = arrived_at
                packet.delivered_at = arrived_at
                ack_delay_ms = link.ack_delay_ms
                packet.ack_delay_ms_used = ack_delay_ms
                link.arrived_buffer[packet.link_dispatch_seq] = packet.id
                self._handoff_arrived_locked(link)
                self._refresh_link_phase_locked(link)

            await asyncio.sleep(max(0, ack_delay_ms) / 1000)

            async with self._lock:
                link = self.links.get(link_id)
                packet = self._packets.get(packet_id)
                if link is None or packet is None:
                    return
                packet.status = "acked"
                packet.acked_at = utcnow()
                link.acked += 1
                # The window slot is released only when the acknowledgement
                # actually comes back.
                link.in_flight.pop(packet_id, None)
                self._on_packet_acked(packet)
                # Fill the freed slot immediately from the original queue.
                self._pump_locked(link)
        except asyncio.CancelledError:
            raise

    # ------------------------------------------------------------------ links
    async def link_detail(self, link_id: str) -> dict:
        if link_id not in self.links:
            raise DrillError(404, f"unknown link {link_id!r}", {"links": list(LINK_IDS)})
        async with self._lock:
            return self.links[link_id].to_dict(self._packets)

    async def update_link(
        self,
        link_id: str,
        delivery_delay_ms: int | None = None,
        ack_delay_ms: int | None = None,
        window_size: int | None = None,
    ) -> dict:
        if link_id not in self.links:
            raise DrillError(404, f"unknown link {link_id!r}", {"links": list(LINK_IDS)})
        if window_size is not None and not (
            isinstance(window_size, int)
            and not isinstance(window_size, bool)
            and MIN_WINDOW <= window_size <= MAX_WINDOW
        ):
            raise DrillError(
                400,
                f"window_size must be an integer between {MIN_WINDOW} and {MAX_WINDOW}",
                {"min_window": MIN_WINDOW, "max_window": MAX_WINDOW},
            )
        async with self._lock:
            link = self.links[link_id]
            if delivery_delay_ms is not None:
                link.delivery_delay_ms = delivery_delay_ms
            if ack_delay_ms is not None:
                link.ack_delay_ms = ack_delay_ms
            if window_size is not None:
                link.window_size = window_size
                # Raising the limit opens slots right away; lowering it takes
                # effect immediately as the pump only fills while occupancy is
                # strictly below the new limit.
                self._pump_locked(link)
            self._log(
                "link_updated",
                link=link_id,
                delivery_delay_ms=link.delivery_delay_ms,
                ack_delay_ms=link.ack_delay_ms,
                window_size=link.window_size,
                in_flight_count=len(link.in_flight),
            )
            return link.to_dict(self._packets)

    # ------------------------------------------------------------------ flows
    async def create_flow(self, flow_id: str | None, link: str) -> dict:
        if link not in LINK_IDS:
            raise DrillError(400, f"link must be one of {LINK_IDS}")
        async with self._lock:
            active = self._active_removal()
            if self.links[link].removed:
                raise DrillError(
                    409,
                    f"link {link} has been removed from the bundle; reset the drill to start over",
                    {"link": link},
                )
            if active is not None and link == active.member_link:
                raise DrillError(
                    409,
                    "cannot create a flow on the member link while it is being removed",
                    {"removal_id": active.id},
                )
            fid = flow_id or f"flow-{uuid.uuid4().hex[:8]}"
            if fid in self.flows:
                raise DrillError(409, f"flow {fid!r} already exists")
            if not isinstance(fid, str) or not fid.strip():
                raise DrillError(400, "flow_id must be a non-empty string")
            flow = Flow(id=fid, link=link, created_at=utcnow())
            self.flows[fid] = flow
            self._log("flow_created", flow_id=fid, link=link)
            return flow.to_dict()

    def _get_flow(self, flow_id: str) -> Flow:
        flow = self.flows.get(flow_id)
        if flow is None:
            raise DrillError(404, f"unknown flow {flow_id!r}")
        return flow

    # ---------------------------------------------------------------- sending
    async def send_packet(self, flow_id: str, seq: int, payload: Any = None) -> dict:
        if seq <= 0:
            raise DrillError(400, "seq must be a positive integer")
        async with self._lock:
            flow = self._get_flow(flow_id)
            if seq in flow.seen_seqs:
                raise DrillError(
                    409,
                    "duplicate seq for this flow; the original packet was kept and this copy was not accepted",
                    {"flow_id": flow_id, "seq": seq},
                )

            active = self._active_removal()
            buffering = (
                active is not None
                and active.status == "pending"
                and flow_id in active.affected_flow_ids
            )

            packet = Packet(
                id=f"pkt-{uuid.uuid4().hex[:12]}",
                flow_id=flow_id,
                seq=seq,
                payload=payload,
                accepted_at=utcnow(),
                accept_index=flow.accepted_count + 1,
            )

            if buffering:
                assert active is not None
                if len(active.buffered_packet_ids) >= active.buffer_limit:
                    active.rejected_count += 1
                    active.add_event(
                        "packet_rejected",
                        flow_id=flow_id,
                        seq=seq,
                        reason="buffer_full",
                        buffered=len(active.buffered_packet_ids),
                        buffer_limit=active.buffer_limit,
                    )
                    raise DrillError(
                        429,
                        "buffer full for the pending link removal; packet rejected",
                        {
                            "removal_id": active.id,
                            "flow_id": flow_id,
                            "seq": seq,
                            "buffer_limit": active.buffer_limit,
                            "buffered_count": len(active.buffered_packet_ids),
                        },
                    )
                packet.status = "buffered"
                packet.buffered_for_removal = active.id
                active.buffered_packet_ids.append(packet.id)
                active.buffered_counts[flow_id] = (
                    active.buffered_counts.get(flow_id, 0) + 1
                )
                active.add_event(
                    "packet_buffered",
                    packet_id=packet.id,
                    flow_id=flow_id,
                    seq=seq,
                    buffered=len(active.buffered_packet_ids),
                )
            else:
                packet.status = "queued"
                packet.link = flow.link
                self._enqueue(packet, flow.link)

            flow.seen_seqs.add(seq)
            flow.accepted_count += 1
            self._packets[packet.id] = packet
            self._log(
                "packet_accepted",
                packet_id=packet.id,
                flow_id=flow_id,
                seq=seq,
                link=packet.link,
                state=packet.status,
            )
            return {
                "accepted": True,
                "buffered": packet.status == "buffered",
                "removal_id": active.id if buffering else None,
                "packet": packet.to_dict(),
                "flow": flow.to_dict(),
                "link_state": (
                    self.links[packet.link].to_dict(self._packets)
                    if packet.link in self.links
                    else None
                ),
            }

    def _enqueue(self, packet: Packet, link_id: str) -> None:
        link = self.links[link_id]
        packet.link = link_id
        packet.status = "queued"
        link.enqueued += 1
        link.queue.put_nowait(packet)
        # Send immediately if a window slot is free.
        self._pump_locked(link)

    # ------------------------------------------------------------ peer / views
    async def received_for_flow(self, flow_id: str) -> dict:
        async with self._lock:
            flow = self._get_flow(flow_id)
            return {
                "flow_id": flow_id,
                "link": flow.link,
                "created_at": flow.created_at,
                "accepted_count": flow.accepted_count,
                "delivery_order": list(flow.received),
                "count": len(flow.received),
            }

    async def packet_detail(self, packet_id: str) -> dict:
        async with self._lock:
            packet = self._packets.get(packet_id)
            if packet is None:
                raise DrillError(404, f"unknown packet {packet_id!r}")
            return packet.to_dict()

    # --------------------------------------------------------------- removals
    def _active_removal(self) -> RemovalRequest | None:
        if self.active_removal_id is None:
            return None
        return self.removals.get(self.active_removal_id)

    async def create_removal(
        self, member_link: str, wait_ms: int, buffer_limit: int
    ) -> dict:
        if member_link not in LINK_IDS:
            raise DrillError(400, f"member_link must be one of {LINK_IDS}")
        target_link = "B" if member_link == "A" else "A"
        if wait_ms < 0:
            raise DrillError(400, "wait_ms must be >= 0")
        if buffer_limit < 0:
            raise DrillError(400, "buffer_limit must be >= 0")

        async with self._lock:
            active = self._active_removal()
            if active is not None and active.status == "pending":
                raise DrillError(
                    409,
                    "another link removal is already pending; only one at a time",
                    {"removal_id": active.id},
                )
            if self.links[member_link].removed:
                raise DrillError(
                    409,
                    f"link {member_link} is no longer part of the bundle",
                    {"link": member_link},
                )
            target = self.links[target_link]
            if target.removed:
                raise DrillError(
                    409,
                    f"target link {target_link} has been removed; nothing to move onto",
                    {"target_link": target_link},
                )

            removal = RemovalRequest(
                id=f"rm-{uuid.uuid4().hex[:10]}",
                member_link=member_link,
                target_link=target_link,
                wait_ms=wait_ms,
                buffer_limit=buffer_limit,
                created_at=utcnow(),
            )
            affected = [f for f in self.flows.values() if f.link == member_link]
            removal.affected_flow_ids = [f.id for f in affected]
            # Everything accepted on the member link before this request must be
            # acknowledged by the peer before the flows may move. This includes
            # queued and in-flight (delivering / physically delivered) packets.
            snapshot = [
                p
                for p in self._packets.values()
                if p.flow_id in removal.affected_flow_ids
                and p.status in {"queued", "delivering", "delivered"}
                and p.buffered_for_removal is None
            ]
            snapshot.sort(key=lambda p: (p.flow_id, p.accept_index))
            removal.snapshot_packet_ids = [p.id for p in snapshot]
            removal.snapshot_acked = 0
            self.removals[removal.id] = removal
            self._removal_order.append(removal.id)
            self.active_removal_id = removal.id
            removal.add_event(
                "removal_created",
                member_link=member_link,
                target_link=target_link,
                wait_ms=wait_ms,
                buffer_limit=buffer_limit,
                affected_flow_ids=list(removal.affected_flow_ids),
                snapshot_packet_ids=list(removal.snapshot_packet_ids),
            )
            self._log(
                "removal_created",
                removal_id=removal.id,
                member_link=member_link,
                snapshot_count=len(snapshot),
            )

            if all(self._packet_acked(pid) for pid in removal.snapshot_packet_ids):
                self._finish_removal(removal, "succeeded", "peer_confirmed")
            else:
                removal.timeout_task = asyncio.create_task(
                    self._removal_timeout(removal.id), name=f"timeout-{removal.id}"
                )
            return removal.to_dict()

    def _packet_acked(self, packet_id: str) -> bool:
        packet = self._packets.get(packet_id)
        return packet is not None and packet.status == "acked"

    def _on_packet_acked(self, packet: Packet) -> None:
        for removal in self.removals.values():
            if packet.id not in removal.snapshot_packet_ids:
                continue
            if removal.status == "pending":
                removal.snapshot_acked += 1
                # Every snapshot packet must be acknowledged; an out-of-order
                # ACK (e.g. a later packet confirmed first) never finishes the
                # removal on its own.
                if all(self._packet_acked(pid) for pid in removal.snapshot_packet_ids):
                    self._finish_removal(removal, "succeeded", "peer_confirmed")
            elif removal.status == "timed_out":
                removal.late_confirmations_ignored += 1
                removal.late_confirmation_packet_ids.append(packet.id)
                removal.add_event(
                    "late_confirmation_ignored",
                    packet_id=packet.id,
                    flow_id=packet.flow_id,
                    seq=packet.seq,
                )

    async def _removal_timeout(self, removal_id: str) -> None:
        removal = self.removals[removal_id]
        await asyncio.sleep(removal.wait_ms / 1000)
        async with self._lock:
            if removal.status != "pending":
                return
            self._finish_removal(removal, "timed_out", "wait_timeout")

    def _finish_removal(
        self, removal: RemovalRequest, status: str, reason: str
    ) -> None:
        """Finalize a removal. Must be called while holding the engine lock."""
        if removal.status != "pending":
            return
        removal.status = status
        removal.result = status
        removal.reason = reason
        removal.finished_at = utcnow()
        timeout_task = removal.timeout_task
        if (
            timeout_task is not None
            and timeout_task is not asyncio.current_task()
            and not timeout_task.done()
        ):
            timeout_task.cancel()
        removal.timeout_task = None
        if self.active_removal_id == removal.id:
            self.active_removal_id = None

        if status == "succeeded":
            for flow_id in removal.affected_flow_ids:
                flow = self.flows.get(flow_id)
                if flow is None:
                    continue
                flow.link = removal.target_link
                removal.moved_flow_ids.append(flow_id)
            self.links[removal.member_link].removed = True
            flush_link = removal.target_link
        else:
            flush_link = removal.member_link

        flushed: list[Packet] = []
        for packet_id in removal.buffered_packet_ids:
            packet = self._packets.get(packet_id)
            if packet is None or packet.status != "buffered":
                continue
            flushed.append(packet)
        flushed.sort(key=lambda p: (p.flow_id, p.accept_index))
        for packet in flushed:
            packet.buffered_for_removal = None
            # _enqueue pumps the window so flushed packets start as slots free.
            self._enqueue(packet, flush_link)
            removal.flushed_packet_ids.append(packet.id)

        removal.buffered_packet_ids = []
        removal.flushed_to = flush_link
        removal.add_event(
            "removal_finished",
            status=status,
            reason=reason,
            moved_flow_ids=list(removal.moved_flow_ids),
            flushed_packet_ids=list(removal.flushed_packet_ids),
            flushed_to=flush_link,
            snapshot_acked=removal.snapshot_acked,
            rejected_count=removal.rejected_count,
        )
        self._log(
            "removal_finished",
            removal_id=removal.id,
            status=status,
            reason=reason,
            moved=len(removal.moved_flow_ids),
            flushed=len(removal.flushed_packet_ids),
            flushed_to=flush_link,
        )

    # -------------------------------------------------------------- snapshots
    async def state(self) -> dict:
        async with self._lock:
            active = self._active_removal()
            return {
                "links": [
                    self.links[lid].to_dict(self._packets) for lid in LINK_IDS
                ],
                "flows": [flow.to_dict() for flow in self.flows.values()],
                "active_removal": active.to_summary() if active else None,
                "removal_count": len(self.removals),
                "events": list(self.events[-100:]),
            }

    async def list_removals(self) -> list[dict]:
        async with self._lock:
            return [
                self.removals[rid].to_summary()
                for rid in reversed(self._removal_order)
            ]

    async def get_removal(self, removal_id: str) -> dict:
        async with self._lock:
            removal = self.removals.get(removal_id)
            if removal is None:
                raise DrillError(404, f"unknown removal {removal_id!r}")
            return removal.to_dict()

    async def reset(self) -> dict:
        await self.stop()
        async with self._lock:
            self._reset_state()
            self._log("system_reset")
        await self.start()
        async with self._lock:
            return {
                "reset": True,
                "window_size": DEFAULT_WINDOW,
                "links": [
                    self.links[lid].to_dict(self._packets) for lid in LINK_IDS
                ],
            }
