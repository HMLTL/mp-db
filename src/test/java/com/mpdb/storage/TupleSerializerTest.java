package com.mpdb.storage;

import com.mpdb.catalog.ColumnDefinition;
import com.mpdb.catalog.ColumnType;
import com.mpdb.catalog.TableSchema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TupleSerializerTest {

    private final TupleSerializer serializer = new TupleSerializer();

    @Test
    void roundTrip_intColumn() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("id", ColumnType.INT)
        ));
        Tuple original = new Tuple(schema, new Object[]{42});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals(42, restored.getValue(0));
    }

    @Test
    void roundTrip_varcharColumn() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("name", ColumnType.VARCHAR, 100)
        ));
        Tuple original = new Tuple(schema, new Object[]{"Hello World"});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals("Hello World", restored.getValue(0));
    }

    @Test
    void roundTrip_booleanColumn() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("flag", ColumnType.BOOLEAN)
        ));
        Tuple original = new Tuple(schema, new Object[]{true});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals(true, restored.getValue(0));
    }

    @Test
    void roundTrip_allTypes() {
        TableSchema schema = new TableSchema("users", List.of(
                new ColumnDefinition("id", ColumnType.INT),
                new ColumnDefinition("name", ColumnType.VARCHAR, 50),
                new ColumnDefinition("active", ColumnType.BOOLEAN)
        ));
        Tuple original = new Tuple(schema, new Object[]{1, "Alice", true});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals(1, restored.getValue(0));
        assertEquals("Alice", restored.getValue(1));
        assertEquals(true, restored.getValue(2));
    }

    @Test
    void roundTrip_emptyString() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("name", ColumnType.VARCHAR, 50)
        ));
        Tuple original = new Tuple(schema, new Object[]{""});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals("", restored.getValue(0));
    }

    @Test
    void roundTrip_unicodeString() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("name", ColumnType.VARCHAR, 100)
        ));
        Tuple original = new Tuple(schema, new Object[]{"Hello"});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals("Hello", restored.getValue(0));
    }

    @Test
    void roundTrip_negativeInt() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("val", ColumnType.INT)
        ));
        Tuple original = new Tuple(schema, new Object[]{-100});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals(-100, restored.getValue(0));
    }

    @Test
    void roundTrip_booleanFalse() {
        TableSchema schema = new TableSchema("t", List.of(
                new ColumnDefinition("flag", ColumnType.BOOLEAN)
        ));
        Tuple original = new Tuple(schema, new Object[]{false});

        byte[] data = serializer.serialize(original);
        Tuple restored = serializer.deserialize(data, schema);

        assertEquals(false, restored.getValue(0));
    }
}
