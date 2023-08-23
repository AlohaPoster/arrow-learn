package org.example.arrow.io;

/**
 * @author Zhang Ruilin <zhangruilin03@kuaishou.com>
 * Created on 2023-08-14
 */

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

public class BinaryDataGenerate {
    private static int columnNum = 200;

    private static int[] batchSizes = new int[]{1, 1, 5, 20, 200, 2000, 10000, 50000, 100000};

    private static long baselineTime = 1L;

    public static List<List<String>> GenerateData(int columnNum, int batchSize) {
        Random random = new Random();
        List<List<String>> data = new ArrayList<>(batchSize);
        for (List<String> rowData : data) {
            rowData = new ArrayList<>(columnNum);
            for (int i = 0; i < columnNum; i++) {
                rowData.add("RandomStringTest" + String.valueOf(i));
            }
        }
        return data;
    }

    public static void main(String[] args) {
        for (int batchSize : batchSizes) {
            List<List<String>> data = GenerateData(columnNum, batchSize);
            try (BufferAllocator allocator = new RootAllocator()) {
                List<FieldVector> vectors = new ArrayList<>();
                List<Field> fields = new ArrayList<>();
                Schema schema = new Schema(fields);
                for (int i = 0; i < columnNum; i++) {
                    VarCharVector vector = new VarCharVector("testvector" + String.valueOf(i), allocator);
                    vectors.add(vector);
                    fields.add(new Field(vector.getName(), FieldType.nullable(Types.MinorType.VARBINARY.getType()), null));
                    vector.allocateNew(batchSize);
                    for (int j = 0; j < batchSize; j++) {
                        vector.setSafe(j, ("RandomStringTest" + String.valueOf(i)).getBytes());
                    }
                    vector.setValueCount(batchSize);
                }
                VectorSchemaRoot root = new VectorSchemaRoot(fields, vectors);
                long begin = System.currentTimeMillis();
                byte[] result = JoinDataSerializer.toBytes(root);
                long end = System.currentTimeMillis();
                long spendTime = end - begin;
                if (batchSize == 1) {
                    baselineTime = spendTime;
                }

                System.out.println("batchsize " + String.valueOf(batchSize) + " timeSpend : " + String.valueOf(spendTime) + " timesWithBaseline : " + String.valueOf(spendTime / baselineTime));
                for (FieldVector vector : vectors) {
                    vector.clear();
                }
            }

        }
    }


}
