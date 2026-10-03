package io.github.retrooper.packetevents.minestom;

import io.github.retrooper.packetevents.minestom.netty.buffer.PacketBuffer;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PacketBufferTest {

    @Test
    void roundTripsPrimitives() {
        PacketBuffer buffer = PacketBuffer.allocate(0);
        buffer.writeByte(-7);
        buffer.writeShort(-12345);
        buffer.writeShortLE(0x1234);
        buffer.writeMedium(-70000);
        buffer.writeMedium(0x7FFFFF);
        buffer.writeInt(Integer.MIN_VALUE);
        buffer.writeLong(Long.MAX_VALUE);

        assertEquals(23, buffer.readableBytes());
        assertEquals(-7, buffer.readByte());
        assertEquals(-12345, buffer.readShort());
        assertEquals(0x3412, buffer.readShort());
        assertEquals(-70000, buffer.readMedium());
        assertEquals(0x7FFFFF, buffer.readMedium());
        assertEquals(Integer.MIN_VALUE, buffer.readInt());
        assertEquals(Long.MAX_VALUE, buffer.readLong());
        assertFalse(buffer.isReadable());
    }

    @Test
    void rejectsReadsPastWrittenBytes() {
        PacketBuffer buffer = PacketBuffer.allocate(64);
        buffer.writeShort(1);

        assertThrows(IndexOutOfBoundsException.class, buffer::readInt);
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.skipBytes(3));
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.readBytes(new byte[3], 0, 3));
        assertThrows(IndexOutOfBoundsException.class, () -> buffer.getUnsignedByte(2));
    }

    @Test
    void transfersBytes() {
        byte[] bytes = "packetevents".getBytes(StandardCharsets.UTF_8);
        PacketBuffer source = PacketBuffer.wrap(bytes);
        PacketBuffer target = PacketBuffer.allocate();
        target.writeBytes(bytes, 6, 6);
        target.writeBytes(source);

        assertFalse(source.isReadable());
        assertEquals("eventspacketevents", target.toString(0, target.writerIndex(), StandardCharsets.UTF_8));
        assertEquals('v', target.getUnsignedByte(1));

        byte[] head = new byte[6];
        target.readBytes(head, 0, head.length);
        assertArrayEquals("events".getBytes(StandardCharsets.UTF_8), head);

        PacketBuffer slice = target.readSlice(6);
        assertEquals("packet", slice.toString(0, 6, StandardCharsets.UTF_8));
        PacketBuffer tail = target.readBytes(6);
        assertEquals("events", tail.toString(0, 6, StandardCharsets.UTF_8));
        tail.writeInt(1);
        assertEquals(10, tail.writerIndex());
    }

    @Test
    void copiesIndependently() {
        PacketBuffer buffer = PacketBuffer.allocate();
        buffer.writeInt(1);
        buffer.writeInt(2);
        buffer.readInt();

        PacketBuffer copy = buffer.copy();
        PacketBuffer clone = buffer.retainedDuplicate();
        PacketBuffer view = buffer.duplicate();
        buffer.clear();
        buffer.writeInt(9);
        buffer.writeInt(9);

        assertEquals(2, copy.readInt());
        assertEquals(4, clone.readerIndex());
        assertEquals(2, clone.readInt());
        assertEquals(9, view.readInt());
    }

    @Test
    void tracksMarksCapacityAndReferences() {
        PacketBuffer buffer = PacketBuffer.allocate(4);
        buffer.writeInt(5);
        buffer.markReaderIndex();
        buffer.markWriterIndex();
        buffer.writeLong(6);
        buffer.readInt();
        buffer.resetReaderIndex();
        buffer.resetWriterIndex();
        assertEquals(0, buffer.readerIndex());
        assertEquals(4, buffer.writerIndex());

        buffer.capacity(128);
        assertEquals(128, buffer.capacity());
        assertEquals(124, buffer.writableBytes());
        buffer.capacity(2);
        assertEquals(2, buffer.writerIndex());

        assertEquals(1, buffer.references());
        buffer.retain();
        assertFalse(buffer.release());
        assertTrue(buffer.release());
        assertEquals(0, buffer.references());
    }
}
