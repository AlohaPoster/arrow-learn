package org.example.arrow.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;

import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;

/**
 * @author Zhang Ruilin <zhangruilin03@kuaishou.com>
 * Created on 2023-08-14
 */
public class JoinDataSerializer {
    public static byte[] toBytes(VectorSchemaRoot table) {
        if (table == null) {
            return new byte[0];
        }
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ArrowStreamWriter writer = new ArrowStreamWriter(table, /*DictionaryProvider=*/null,
                    Channels.newChannel(out));
            writer.start();
            writer.writeBatch();
            writer.end();
            return out.toByteArray();
        } catch (Exception exception) {
            throw new RuntimeException("Could not serialize VectorSchemaRoot: ", exception);
        }
    }

//    public static VectorSchemaRoot fromBytes(VectorManager vectorManager, byte[] bytes) {
//        try {
//            ArrowStreamReader reader = new ArrowStreamReader(
//                    new ByteArrayInputStream(bytes), vectorManager.getAllocator());
//            reader.loadNextBatch();
//            VectorSchemaRoot table = reader.getVectorSchemaRoot();
//            vectorManager.appendVectors(table.getFieldVectors());
//            return table;
//        } catch (Exception exception) {
//            throw new RuntimeException("Could not deserialize VectorSchemaRoot: ", exception);
//        }
//    }

}
