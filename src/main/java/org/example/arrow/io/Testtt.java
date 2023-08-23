package org.example.arrow.io;

import java.util.ArrayList;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.VectorSchemaRootAppender;

/**
 * @author Zhang Ruilin <zhangruilin03@kuaishou.com>
 * Created on 2023-08-23
 */
public class Testtt {

    private static void soutSchemaRoot(VectorSchemaRoot schemaRoot) {
        for (FieldVector vector : schemaRoot.getFieldVectors()) {
            String columnName = vector.getName();
            int valueCount = vector.getValueCount();

            System.out.println("Column Name: " + columnName + "valueCount : " + valueCount);

            // Retrieve the values of the vector
            for (int i = 0; i < valueCount; i++) {
                if (vector.isNull(i)) {
                    System.out.println("NULL");
                } else {
                    Object value = vector.getObject(i);
                    System.out.println("Value: " + value);
                }
            }
        }
    }

    public static void main(String[] args) {
        try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)) {
            // Create column vectors
            BigIntVector col1 = new BigIntVector("col1", allocator);
            Float8Vector col2 = new Float8Vector("col2", allocator);

            col1.setSafe(0, 10);
            col2.setSafe(0, 10.8f);

            Float8Vector col5 = new Float8Vector("col5", allocator);
            BigIntVector col3 = new BigIntVector("col3", allocator);
            Float8Vector col4 = new Float8Vector("col4", allocator);

            col3.setSafe(0, 199);
            col4.setSafe(0, 66.8f);
            col5.setSafe(0, 777.7f);
            // Create VectorSchemaRoot
            VectorSchemaRoot schemaRoot = new VectorSchemaRoot(new ArrayList<FieldVector>() {{
                add(col1); add(col2);
            }});
            schemaRoot.setRowCount(1);
            VectorSchemaRoot schemaRoot2 = new VectorSchemaRoot(new ArrayList<FieldVector>() {{
                add(col3); add(col4); add(col5);
            }});
            schemaRoot2.setRowCount(1);

            // Create VectorSchemaRootAppender
            VectorSchemaRootAppender.append(false, schemaRoot, schemaRoot2);
            schemaRoot2.close();
            soutSchemaRoot(schemaRoot);
            schemaRoot.close();

        }
    }
//    public static void main(String[] arg) {
//        String pp = null;
//        if (pp == null || pp.length() == 0) {
////            System.out.println("jjjjjjj");
//        }
//
//        int p = 14;
//        switch (p) {
//            case 1 :
//                String ppp = "jjjj";
//                System.out.println(ppp);
//            case 10 :
//                ppp = "oooooo";
//                System.out.println(ppp);
//            case 14 :
//                ppp = "oslodiuoifuei";
//                System.out.println(ppp);
//        }
//        System.out.println(p);
//    }
//
//    private static String kk(int p) {
//        switch (p) {
//            case 1 :
//                String ppp = "jjjj";
//                return ppp;
//            case 10 :
//                ppp = "oooooo";
//                return ppp;
//        }
//        return "jdkljfsljdf";
//    }
}
