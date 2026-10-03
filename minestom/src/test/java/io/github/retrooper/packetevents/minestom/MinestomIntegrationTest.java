package io.github.retrooper.packetevents.minestom;

import com.github.retrooper.packetevents.PacketEvents;
import com.github.retrooper.packetevents.event.PacketListener;
import com.github.retrooper.packetevents.event.PacketListenerCommon;
import com.github.retrooper.packetevents.event.PacketListenerPriority;
import com.github.retrooper.packetevents.event.PacketReceiveEvent;
import com.github.retrooper.packetevents.event.PacketSendEvent;
import com.github.retrooper.packetevents.manager.server.ServerVersion;
import com.github.retrooper.packetevents.netty.channel.ChannelHelper;
import com.github.retrooper.packetevents.protocol.ConnectionState;
import com.github.retrooper.packetevents.protocol.entity.type.EntityTypes;
import com.github.retrooper.packetevents.protocol.item.type.ItemTypes;
import com.github.retrooper.packetevents.protocol.packettype.PacketType;
import com.github.retrooper.packetevents.protocol.packettype.PacketTypeCommon;
import com.github.retrooper.packetevents.protocol.player.User;
import com.github.retrooper.packetevents.protocol.world.states.WrappedBlockState;
import com.github.retrooper.packetevents.protocol.world.states.type.StateTypes;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientChatMessage;
import com.github.retrooper.packetevents.wrapper.play.client.WrapperPlayClientHeldItemChange;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerEntityPositionSync;
import com.github.retrooper.packetevents.wrapper.play.server.WrapperPlayServerSystemChatMessage;
import io.github.retrooper.packetevents.minestom.factory.MinestomPacketEventsBuilder;
import io.github.retrooper.packetevents.minestom.util.MinestomConversionUtil;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.serializer.plain.PlainTextComponentSerializer;
import net.minestom.server.MinecraftServer;
import net.minestom.server.component.DataComponents;
import net.minestom.server.coordinate.Pos;
import net.minestom.server.entity.Entity;
import net.minestom.server.entity.EntityType;
import net.minestom.server.entity.GameMode;
import net.minestom.server.entity.Player;
import net.minestom.server.event.player.AsyncPlayerConfigurationEvent;
import net.minestom.server.event.player.PlayerChangeHeldSlotEvent;
import net.minestom.server.event.player.PlayerChatEvent;
import net.minestom.server.instance.Instance;
import net.minestom.server.instance.block.Block;
import net.minestom.server.item.ItemStack;
import net.minestom.server.item.Material;
import net.minestom.server.network.packet.client.common.ClientPingRequestPacket;
import net.minestom.server.network.packet.client.handshake.ClientHandshakePacket;
import net.minestom.server.network.packet.client.play.ClientChatMessagePacket;
import net.minestom.server.network.packet.client.status.StatusRequestPacket;
import net.minestom.server.network.packet.server.ServerPacket;
import net.minestom.server.network.packet.server.common.PingResponsePacket;
import net.minestom.server.network.packet.server.play.EntityPositionSyncPacket;
import net.minestom.server.network.packet.server.play.SystemChatPacket;
import net.minestom.server.network.packet.server.status.ResponsePacket;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MinestomIntegrationTest {

    private static final Queue<String> CHAT = new ConcurrentLinkedQueue<>();
    private static final Queue<Integer> SLOTS = new ConcurrentLinkedQueue<>();
    private static final AtomicInteger NAMES = new AtomicInteger();

    private static Instance instance;
    private static int port;

    private final List<PacketListenerCommon> listeners = new ArrayList<>();
    private PacketRecorder recorder;

    @BeforeAll
    static void start() {
        MinecraftServer server = MinecraftServer.init();
        instance = MinecraftServer.getInstanceManager().createInstanceContainer();
        instance.setGenerator(unit -> unit.modifier().fillHeight(0, 40, Block.STONE));
        MinecraftServer.getGlobalEventHandler()
                .addListener(AsyncPlayerConfigurationEvent.class, event -> {
                    event.setSpawningInstance(instance);
                    event.getPlayer().setRespawnPoint(new Pos(0, 42, 0));
                })
                .addListener(PlayerChatEvent.class, event -> CHAT.add(event.getRawMessage()))
                .addListener(PlayerChangeHeldSlotEvent.class, event -> SLOTS.add((int) event.getNewSlot()));

        PacketEvents.setAPI(MinestomPacketEventsBuilder.build());
        PacketEvents.getAPI().getSettings().checkForUpdates(false);
        PacketEvents.getAPI().load();
        PacketEvents.getAPI().init();

        server.start("127.0.0.1", 0);
        port = MinecraftServer.getServer().getPort();
    }

    @AfterAll
    static void stop() {
        PacketEvents.getAPI().terminate();
        MinecraftServer.stopCleanly();
    }

    @BeforeEach
    void register() {
        this.recorder = new PacketRecorder();
        listen(this.recorder);
        CHAT.clear();
        SLOTS.clear();
    }

    @AfterEach
    void unregister() {
        for (PacketListenerCommon listener : this.listeners) {
            PacketEvents.getAPI().getEventManager().unregisterListener(listener);
        }
        this.listeners.clear();
    }

    @Test
    void resolvesServerVersion() {
        ServerVersion version = PacketEvents.getAPI().getServerManager().getVersion();
        assertEquals(MinecraftServer.PROTOCOL_VERSION, version.getProtocolVersion());
    }

    @Test
    void interceptsStatusExchange() throws Exception {
        try (TestClient client = new TestClient(port)) {
            client.handshake(ClientHandshakePacket.Intent.STATUS, new StatusRequestPacket(), new ClientPingRequestPacket(7));
            assertNotNull(client.await(ResponsePacket.class));
            assertEquals(7, client.await(PingResponsePacket.class).number());
        }
        PacketRecorder.await(() -> !this.recorder.connected.isEmpty());
        User user = this.recorder.connected.peek();
        PacketRecorder.await(() -> this.recorder.disconnected.contains(user));

        assertEquals(List.of(PacketType.Handshaking.Client.HANDSHAKE, PacketType.Status.Client.REQUEST,
                PacketType.Status.Client.PING), this.recorder.received(user));
        assertEquals(List.of(PacketType.Status.Server.RESPONSE, PacketType.Status.Server.PONG),
                this.recorder.sent(user));
        assertEquals(1, this.recorder.connected.size());
        assertEquals(1, this.recorder.addresses.size());
        assertNull(PacketEvents.getAPI().getProtocolManager().getUser(user.getChannel()));
    }

    @Test
    void tracksLoginThroughPlay() throws Exception {
        String name = name();
        try (TestClient client = new TestClient(port)) {
            client.join(name);
            PacketRecorder.await(() -> !this.recorder.loggedIn.isEmpty());

            Player player = MinecraftServer.getConnectionManager().getOnlinePlayerByUsername(name);
            User user = PacketEvents.getAPI().getPlayerManager().getUser(player);
            assertNotNull(user);
            assertSame(user, this.recorder.loggedIn.peek());
            assertEquals(name, user.getName());
            assertEquals(player.getUuid(), user.getUUID());
            assertEquals(ConnectionState.PLAY, user.getDecoderState());
            assertEquals(ConnectionState.PLAY, user.getEncoderState());
            assertEquals(MinecraftServer.PROTOCOL_VERSION, user.getClientVersion().getProtocolVersion());
            assertEquals(player.getEntityId(), user.getEntityId());
            assertTrue(ChannelHelper.isOpen(user.getChannel()));
            assertSame(user.getChannel(), PacketEvents.getAPI().getPlayerManager().getChannel(player));
            assertTrue(PacketEvents.getAPI().getInjector().isPlayerSet(user.getChannel()));

            List<PacketTypeCommon> received = this.recorder.received(user);
            List<PacketTypeCommon> sent = this.recorder.sent(user);
            assertEquals(PacketType.Handshaking.Client.HANDSHAKE, received.get(0));
            assertEquals(PacketType.Login.Client.LOGIN_START, received.get(1));
            assertTrue(received.contains(PacketType.Login.Client.LOGIN_SUCCESS_ACK));
            assertTrue(received.contains(PacketType.Configuration.Client.CONFIGURATION_END_ACK));
            assertEquals(PacketType.Login.Server.SET_COMPRESSION, sent.get(0));
            assertEquals(PacketType.Login.Server.LOGIN_SUCCESS, sent.get(1));
            assertTrue(sent.contains(PacketType.Configuration.Server.REGISTRY_DATA));
            assertTrue(sent.contains(PacketType.Configuration.Server.CONFIGURATION_END));
            assertTrue(sent.contains(PacketType.Play.Server.JOIN_GAME));
            assertTrue(sent.contains(PacketType.Play.Server.CHUNK_DATA));
        }
        User user = this.recorder.loggedIn.peek();
        PacketRecorder.await(() -> this.recorder.disconnected.contains(user));
        assertNull(PacketEvents.getAPI().getProtocolManager().getChannel(user.getUUID()));
    }

    @Test
    void rewritesAndCancelsOutgoingPackets() throws Exception {
        listen(new PacketListener() {
            @Override
            public void onPacketSend(PacketSendEvent event) {
                if (event.getPacketType() != PacketType.Play.Server.SYSTEM_CHAT_MESSAGE) {
                    return;
                }
                WrapperPlayServerSystemChatMessage wrapper = new WrapperPlayServerSystemChatMessage(event);
                String text = plain(wrapper.getMessage());
                if (text.equals("original")) {
                    wrapper.setMessage(Component.text("rewritten"));
                } else if (text.equals("cancelled")) {
                    event.setCancelled(true);
                }
            }
        });
        String name = name();
        try (TestClient client = new TestClient(port)) {
            client.join(name);
            Player player = MinecraftServer.getConnectionManager().getOnlinePlayerByUsername(name);
            player.sendMessage(Component.text("original"));
            player.sendMessage(Component.text("cancelled"));
            player.sendMessage(Component.text("marker"));

            assertEquals(List.of("rewritten", "marker"), chat(client, 2));
        }
    }

    @Test
    void sendsPacketsWithNestedOrdering() throws Exception {
        listen(new PacketListener() {
            @Override
            public void onPacketSend(PacketSendEvent event) {
                if (event.getPacketType() != PacketType.Play.Server.SYSTEM_CHAT_MESSAGE) {
                    return;
                }
                WrapperPlayServerSystemChatMessage wrapper = new WrapperPlayServerSystemChatMessage(event);
                if (!plain(wrapper.getMessage()).equals("middle")) {
                    return;
                }
                User user = event.getUser();
                user.sendPacketSilently(new WrapperPlayServerSystemChatMessage(false, Component.text("before")));
                event.getPostTasks().add(() -> user.sendPacketSilently(
                        new WrapperPlayServerSystemChatMessage(false, Component.text("after"))));
            }
        });
        String name = name();
        try (TestClient client = new TestClient(port)) {
            client.join(name);
            Player player = MinecraftServer.getConnectionManager().getOnlinePlayerByUsername(name);
            User user = PacketEvents.getAPI().getPlayerManager().getUser(player);

            this.recorder.reset();
            user.sendPacket(new WrapperPlayServerSystemChatMessage(false, Component.text("loud")));
            user.sendPacketSilently(new WrapperPlayServerSystemChatMessage(false, Component.text("silent")));
            ChannelHelper.runInEventLoop(user.getChannel(), () ->
                    user.writePacketSilently(new WrapperPlayServerSystemChatMessage(false, Component.text("looped"))));
            player.sendMessage(Component.text("middle"));

            assertEquals(List.of("loud", "silent", "looped", "before", "middle", "after"), chat(client, 6));
            int events = 0;
            for (PacketTypeCommon type : this.recorder.sent(user)) {
                if (type == PacketType.Play.Server.SYSTEM_CHAT_MESSAGE) {
                    events++;
                }
            }
            assertEquals(2, events);
        }
    }

    @Test
    void rewritesCancelsAndInjectsIncomingPackets() throws Exception {
        listen(new PacketListener() {
            @Override
            public void onPacketReceive(PacketReceiveEvent event) {
                if (event.getPacketType() != PacketType.Play.Client.CHAT_MESSAGE) {
                    return;
                }
                WrapperPlayClientChatMessage wrapper = new WrapperPlayClientChatMessage(event);
                if (wrapper.getMessage().equals("original")) {
                    wrapper.setMessage("rewritten");
                } else if (wrapper.getMessage().equals("cancelled")) {
                    event.setCancelled(true);
                }
            }
        });
        String name = name();
        try (TestClient client = new TestClient(port)) {
            client.join(name);
            Player player = MinecraftServer.getConnectionManager().getOnlinePlayerByUsername(name);
            User user = PacketEvents.getAPI().getPlayerManager().getUser(player);

            client.send(chat("original"), chat("cancelled"), chat("marker"));
            PacketRecorder.await(() -> CHAT.contains("marker"));
            assertEquals(List.of("rewritten", "marker"), List.copyOf(CHAT));

            this.recorder.reset();
            PacketEvents.getAPI().getProtocolManager().receivePacket(user.getChannel(), new WrapperPlayClientHeldItemChange(4));
            PacketEvents.getAPI().getProtocolManager().receivePacketSilently(user.getChannel(), new WrapperPlayClientHeldItemChange(6));
            PacketRecorder.await(() -> SLOTS.size() == 2);
            assertEquals(List.of(4, 6), List.copyOf(SLOTS));
            int events = 0;
            for (PacketTypeCommon type : this.recorder.received(user)) {
                if (type == PacketType.Play.Client.HELD_ITEM_CHANGE) {
                    events++;
                }
            }
            assertEquals(1, events);
        }
    }

    @Test
    void interceptsViewablePackets() throws Exception {
        Queue<Integer> synced = new ConcurrentLinkedQueue<>();
        listen(new PacketListener() {
            @Override
            public void onPacketSend(PacketSendEvent event) {
                if (event.getPacketType() == PacketType.Play.Server.ENTITY_POSITION_SYNC) {
                    synced.add(new WrapperPlayServerEntityPositionSync(event).getId());
                }
            }
        });
        String name = name();
        try (TestClient client = new TestClient(port)) {
            client.join(name);
            Player player = MinecraftServer.getConnectionManager().getOnlinePlayerByUsername(name);
            PacketRecorder.await(() -> player.getInstance() != null);

            Entity entity = new Entity(EntityType.ARMOR_STAND);
            entity.setInstance(instance, new Pos(2, 42, 2)).join();
            PacketRecorder.await(() -> entity.getViewers().contains(player));
            entity.teleport(new Pos(3, 42, 3)).join();

            int id = entity.getEntityId();
            client.await(packet -> packet instanceof EntityPositionSyncPacket sync && sync.entityId() == id);
            assertTrue(synced.contains(id));
        }
    }

    @Test
    void compressesLargePackets() throws Exception {
        String text = "packetevents".repeat(400);
        listen(new PacketListener() {
            @Override
            public void onPacketSend(PacketSendEvent event) {
                if (event.getPacketType() != PacketType.Play.Server.SYSTEM_CHAT_MESSAGE) {
                    return;
                }
                WrapperPlayServerSystemChatMessage wrapper = new WrapperPlayServerSystemChatMessage(event);
                if (plain(wrapper.getMessage()).equals("grow")) {
                    wrapper.setMessage(Component.text(text));
                }
            }
        });
        String name = name();
        try (TestClient client = new TestClient(port)) {
            client.join(name);
            Player player = MinecraftServer.getConnectionManager().getOnlinePlayerByUsername(name);
            User user = PacketEvents.getAPI().getPlayerManager().getUser(player);

            user.sendPacket(new WrapperPlayServerSystemChatMessage(false, Component.text(text)));
            player.sendMessage(Component.text("grow"));
            player.sendMessage(Component.text(text));

            assertEquals(List.of(text, text, text), chat(client, 3));
        }
    }

    @Test
    void convertsPlatformTypes() {
        ItemStack item = ItemStack.of(Material.DIAMOND_SWORD, 3).with(DataComponents.CUSTOM_NAME, Component.text("Blade"));
        com.github.retrooper.packetevents.protocol.item.ItemStack converted = MinestomConversionUtil.fromMinestomItemStack(item);
        assertEquals(ItemTypes.DIAMOND_SWORD, converted.getType());
        assertEquals(3, converted.getAmount());
        assertEquals(item, MinestomConversionUtil.toMinestomItemStack(converted));

        Block block = Block.OAK_STAIRS.withProperty("facing", "east");
        WrappedBlockState state = MinestomConversionUtil.fromMinestomBlock(block);
        assertEquals(StateTypes.OAK_STAIRS, state.getType());
        assertEquals(block, MinestomConversionUtil.toMinestomBlock(state));

        assertEquals(EntityTypes.ZOMBIE, MinestomConversionUtil.fromMinestomEntityType(EntityType.ZOMBIE));
        assertEquals(EntityType.ZOMBIE, MinestomConversionUtil.toMinestomEntityType(EntityTypes.ZOMBIE));
        assertEquals(ItemTypes.STONE, MinestomConversionUtil.fromMinestomMaterial(Material.STONE));
        assertEquals(Material.STONE, MinestomConversionUtil.toMinestomMaterial(ItemTypes.STONE));
        assertEquals(GameMode.SPECTATOR, MinestomConversionUtil.toMinestomGameMode(
                MinestomConversionUtil.fromMinestomGameMode(GameMode.SPECTATOR)));

        Pos pos = new Pos(1.5, 64, -3.25, 90, 45);
        assertEquals(pos, MinestomConversionUtil.toMinestomPos(MinestomConversionUtil.fromMinestomPos(pos)));
        assertEquals(pos.asVec(), MinestomConversionUtil.toMinestomVec(MinestomConversionUtil.fromMinestomPoint(pos)));
    }

    @Test
    void closesConnections() throws Exception {
        String name = name();
        try (TestClient client = new TestClient(port)) {
            client.join(name);
            Player player = MinecraftServer.getConnectionManager().getOnlinePlayerByUsername(name);
            User user = PacketEvents.getAPI().getPlayerManager().getUser(player);
            user.closeConnection();

            PacketRecorder.await(() -> this.recorder.disconnected.contains(user));
            assertFalse(ChannelHelper.isOpen(user.getChannel()));
        }
    }

    private void listen(PacketListener listener) {
        this.listeners.add(PacketEvents.getAPI().getEventManager().registerListener(listener, PacketListenerPriority.NORMAL));
    }

    private static List<String> chat(TestClient client, int count) throws InterruptedException {
        List<String> messages = new ArrayList<>(count);
        while (messages.size() < count) {
            ServerPacket packet = client.await(SystemChatPacket.class);
            messages.add(plain(((SystemChatPacket) packet).message()));
        }
        return messages;
    }

    private static ClientChatMessagePacket chat(String message) {
        return new ClientChatMessagePacket(message, System.currentTimeMillis(), 0, null, 0, new BitSet(), (byte) 0);
    }

    private static String plain(Component component) {
        return PlainTextComponentSerializer.plainText().serialize(component);
    }

    private static String name() {
        return "Tester" + NAMES.incrementAndGet();
    }
}
