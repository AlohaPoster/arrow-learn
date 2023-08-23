package org.example.arrow.io;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.FieldType;

/**
 * @author yourname <zhangruilin03@kuaishou.com>
 * Created on 2023-07-28
 */
public class ArrowTest {
    public static void main(String[] args) {
        try {
            BufferAllocator allocator = new RootAllocator();
            ListVector listVector = ListVector.empty("listVector", allocator);
            listVector.allocateNew();
            listVector.setValueCount(1);
            System.out.println(listVector.getChildrenFromFields().size());
            System.out.println(listVector.getChildrenFromFields().get(0));
            System.out.println("--------------");
//            listVector.addOrGetVector(FieldType.nullable(Types.MinorType.VARCHAR.getType()));
            System.out.println(listVector.getChildrenFromFields().size());
            System.out.println(listVector.getChildrenFromFields().get(0));
            UnionListWriter listWriter = listVector.getWriter();
            int[] data = new int[] { 1, 2, 3, 10, 20, 30, 100, 200, 300, 1000, 2000, 3000 };
            int tmp_index = 0;
            for (int i = 0; i < 4; i++) {
                listWriter.setPosition(i);
                listWriter.startList();

//                System.out.println(tempBuffer.toHexString(0, s.length));
                for (int j = 0; j < 2; j++) {
                    String str = "Heldddd111";
                    if (j == 0) {
                        str = "hellk000000";
                    }

                    byte[] s = str.getBytes();
                    ArrowBuf tempBuffer = allocator.buffer(s.length);
                    tempBuffer.writeBytes(s);
                    listWriter.writeVarChar(0, s.length, tempBuffer);
                    tmp_index = tmp_index + 1;
                }
//                listWriter.setValueCount(3);
                listWriter.endList();
            }
            listVector.setValueCount(4);

            System.out.print(listVector);
            System.out.println("--------------");
//            System.out.println(listVector.getChildrenFromFields().get(0));
        } catch (Exception e) {
            e.printStackTrace();
        }
//        try (
//                BufferAllocator allocator = new RootAllocator();
//                IntVector intVector = new IntVector("fixed-size-primitive-layout", allocator);
//                VarCharVector bigIntVector = new VarCharVector("kkk", allocator)
//        ) {
//            bigIntVector.allocateNew(3);
//            bigIntVector.set(2, "jdd".getBytes());
//            bigIntVector.setValueCount(3);
//            System.out.println("Vector created in memory: " + bigIntVector);
//
//            intVector.allocateNew(3);
//            intVector.setNull(1);
//            intVector.set(2, 2);
//            intVector.setValueCount(3);
//            System.out.println("Vector created in memory: " + intVector);
//        }
    }
}
