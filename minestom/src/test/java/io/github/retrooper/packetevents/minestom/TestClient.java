package io.github.retrooper.packetevents.minestom;

import net.minestom.server.MinecraftServer;
import net.minestom.server.network.ConnectionState;
import net.minestom.server.network.NetworkBuffer;
import net.minestom.server.network.packet.PacketReading;
import net.minestom.server.network.packet.PacketVanilla;
import net.minestom.server.network.packet.PacketWriting;
import net.minestom.server.network.packet.client.ClientPacket;
import net.minestom.server.network.packet.client.common.ClientKeepAlivePacket;
import net.minestom.server.network.packet.client.configuration.ClientFinishConfigurationPacket;
import net.minestom.server.network.packet.client.configuration.ClientSelectKnownPacksPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.login.ClientLoginAcknowledgedPacket;
import net.minestom.server.network.packet.client.login.ClientLoginStartPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.KeepAlivePacket;
import net.minestom.server.network.packet.server.configuration.FinishConfigurationPacket;
import net.minestom.server.network.packet.server.configuration.SelectKnownPacksPacket;
import net.minestom.server.network.packet.server.login.LoginSuccessPacket;
import net.minestom.server.network.packet.server.login.SetCompressionPacket;
import net.minestom.server.network.packet.server.play.JoinGamePacket;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.zip.DataFormatException;

final class TestClient implements AutoCloseable {

    private static final long TIMEOUT_SECONDS = 10;

    private final SocketChannel channel;
    private final int port;
    private final BlockingQueue<ServerPacket> received = new LinkedBlockingQueue<>();
    private final NetworkBuffer input = NetworkBuffer.resizableBuffer(1 << 16, MinecraftServer.getRegistries());
    private ConnectionState writeState = ConnectionState.HANDSHAKE;
    private ConnectionState readState = ConnectionState.HANDSHAKE;
    private volatile boolean compression;

    TestClient(int port) throws IOException {
        this.port = port;
        this.channel = SocketChannel.open(new InetSocketAddress("127.0.0.1", port));
    }

    void handshake(ClientHandshakePacket.Intent intent, ClientPacket... following) throws IOException {
        this.readState = intent == ClientHandshakePacket.Intent.STATUS ? ConnectionState.STATUS : ConnectionState.LOGIN;
        ClientPacket[] packets = new ClientPacket[following.length + 1];
        packets[0] = new ClientHandshakePacket(MinecraftServer.PROTOCOL_VERSION, "127.0.0.1", this.port, intent);
        System.arraycopy(following, 0, packets, 1, following.length);
        send(packets);
        Thread.ofVirtual().name("test-client-reader").start(this::readLoop);
    }

    void join(String username) throws IOException, InterruptedException {
        handshake(ClientHandshakePacket.Intent.LOGIN, new ClientLoginStartPacket(username, UUID.randomUUID()));
        await(JoinGamePacket.class);
    }

    synchronized void send(ClientPacket... packets) throws IOException {
        NetworkBuffer output = NetworkBuffer.resizableBuffer(1024, MinecraftServer.getRegistries());
        for (ClientPacket packet : packets) {
            int threshold = this.compression ? MinecraftServer.getCompressionThreshold() : 0;
            NetworkBuffer framed = PacketWriting.allocateTrimmedPacket(this.writeState, packet, threshold);
            output.ensureWritable(framed.capacity());
            NetworkBuffer.copy(framed, 0, output, output.writeIndex(), framed.capacity());
            output.advanceWrite(framed.capacity());
            this.writeState = PacketVanilla.nextClientState(packet, this.writeState);
        }
        while (!output.writeChannel(this.channel)) {
            Thread.onSpinWait();
        }
    }

    <T extends ServerPacket> T await(Class<T> type) throws InterruptedException {
        return type.cast(await(type::isInstance));
    }

    ServerPacket await(Predicate<ServerPacket> filter) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (true) {
            ServerPacket packet = this.received.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
            if (packet == null) {
                throw new AssertionError("Timed out waiting for a server packet");
            }
            if (filter.test(packet)) {
                return packet;
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.channel.close();
    }

    private void readLoop() {
        try {
            while (this.channel.isOpen()) {
                this.input.readChannel(this.channel);
                drain();
                this.input.compact();
            }
        } catch (IOException | DataFormatException ignored) {
        }
    }

    private void drain() throws IOException, DataFormatException {
        while (this.input.readableBytes() > 0) {
            PacketReading.Result<ServerPacket> result;
            try {
                result = PacketReading.readServer(this.input, this.readState, this.compression);
            } catch (RuntimeException exception) {
                continue;
            }
            switch (result) {
                case PacketReading.Result.Success<ServerPacket> success -> {
                    PacketReading.ParsedPacket<ServerPacket> parsed = success.packets().getFirst();
                    this.readState = parsed.nextState();
                    handle(parsed.packet());
                }
                case PacketReading.Result.Failure<ServerPacket> failure -> {
                    this.input.compact();
                    if (failure.requiredCapacity() > this.input.capacity()) {
                        this.input.resize(failure.requiredCapacity());
                    }
                    return;
                }
                default -> {
                    return;
                }
            }
        }
    }

    private void handle(ServerPacket packet) throws IOException {
        switch (packet) {
            case SetCompressionPacket _ -> this.compression = true;
            case LoginSuccessPacket _ -> send(new ClientLoginAcknowledgedPacket());
            case SelectKnownPacksPacket packs -> send(new ClientSelectKnownPacksPacket(packs.entries()));
            case FinishConfigurationPacket _ -> send(new ClientFinishConfigurationPacket());
            case KeepAlivePacket keepAlive -> send(new ClientKeepAlivePacket(keepAlive.id()));
            default -> {
            }
        }
        this.received.add(packet);
    }
}
