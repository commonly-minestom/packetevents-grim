package io.github.retrooper.packetevents.minestom;

import com.github.retrooper.packetevents.event.PacketListener;
import com.github.retrooper.packetevents.event.PacketReceiveEvent;
import com.github.retrooper.packetevents.event.PacketSendEvent;
import com.github.retrooper.packetevents.event.UserConnectEvent;
import com.github.retrooper.packetevents.event.UserDisconnectEvent;
import com.github.retrooper.packetevents.event.UserLoginEvent;
import com.github.retrooper.packetevents.protocol.packettype.PacketTypeCommon;
import com.github.retrooper.packetevents.protocol.player.User;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

final class PacketRecorder implements PacketListener {

    private final Queue<Entry> received = new ConcurrentLinkedQueue<>();
    private final Queue<Entry> sent = new ConcurrentLinkedQueue<>();
    final Queue<User> connected = new ConcurrentLinkedQueue<>();
    final Queue<InetSocketAddress> addresses = new ConcurrentLinkedQueue<>();
    final Queue<User> disconnected = new ConcurrentLinkedQueue<>();
    final Queue<User> loggedIn = new ConcurrentLinkedQueue<>();

    @Override
    public void onPacketReceive(PacketReceiveEvent event) {
        this.received.add(new Entry(event.getUser(), event.getPacketType()));
    }

    @Override
    public void onPacketSend(PacketSendEvent event) {
        this.sent.add(new Entry(event.getUser(), event.getPacketType()));
    }

    @Override
    public void onUserConnect(UserConnectEvent event) {
        this.connected.add(event.getUser());
        if (event.getUser().getAddress() != null) {
            this.addresses.add(event.getUser().getAddress());
        }
    }

    @Override
    public void onUserLogin(UserLoginEvent event) {
        this.loggedIn.add(event.getUser());
    }

    @Override
    public void onUserDisconnect(UserDisconnectEvent event) {
        this.disconnected.add(event.getUser());
    }

    List<PacketTypeCommon> received(User user) {
        return filter(this.received, user);
    }

    List<PacketTypeCommon> sent(User user) {
        return filter(this.sent, user);
    }

    void reset() {
        this.received.clear();
        this.sent.clear();
    }

    private static List<PacketTypeCommon> filter(Queue<Entry> entries, User user) {
        List<PacketTypeCommon> types = new ArrayList<>();
        for (Entry entry : entries) {
            if (entry.user() == user) {
                types.add(entry.type());
            }
        }
        return types;
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (!condition.getAsBoolean()) {
            if (System.nanoTime() > deadline) {
                throw new AssertionError("Condition was not met in time");
            }
            Thread.sleep(10);
        }
    }

    private record Entry(User user, PacketTypeCommon type) {
    }
}
