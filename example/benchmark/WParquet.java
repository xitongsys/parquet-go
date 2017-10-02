package Main;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetReader.Builder;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class Main {

    public static void main(String[] args) throws Exception {
        parquetWriter(args[0], Long.parseLong(args[1]));
    }

    static void parquetWriter(String outPath, long num) throws IOException{
        MessageType schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                        " required binary name (UTF8);\n" +
                        " required int32 age;\n " +
                        " required int64 id;\n" +
                        " required double weight;\n" +
                        " required boolean sex;\n" +
                        " required binary school (UTF8);\n" +
                        "}\n"+
                        "}");

        GroupFactory factory = new SimpleGroupFactory(schema);
        Path path = new Path(outPath);
        Configuration configuration = new Configuration();
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        writeSupport.setSchema(schema,configuration);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(path,configuration,writeSupport);

        for ( long i=0; i<num; i++) {
            Group group = factory.newGroup()
                    .append("name", "StudentName")
                    .append("age", 18 + (int)i%10)
                    .append("id", i)
                    .append("weight", 60.0 + (double)i%10)
                    .append("sex", i%2==0)
                    .append( "school", "PKU");
            writer.write(group);

            if (i%(num/100) == 0 ) {
                System.out.println(i*100/num + "%");
            }
        }

        System.out.println("write end");
        writer.close();
    }
}
