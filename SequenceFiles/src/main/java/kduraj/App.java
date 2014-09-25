package kduraj;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class App {

    /*--------------------------------------------------------------------------------------------*/
    static String[] DATA = {
              "key1|one"
            , "key2|two"
            , "key3|three"
            , "key4|four"
            , "key9|nine"
            , "key5|five"
            , "key6|six"
            , "key7|seven"
            , "key8|eight"
	};

    static String uri;
    static Configuration conf;
    static FileSystem fs;
    static Path path;

    /*--------------------------------------------------------------------------------------------    
        String uri = args[0];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);
     --------------------------------------------------------------------------------------------*/
    public static void main(String[] args) throws IOException {
        
        uri = args[0];
        conf = new Configuration();
        fs = FileSystem.get(URI.create(uri), conf);
        path = new Path(uri);
        
        WriteSequenceFile writer = new WriteSequenceFile();        
        writer.key_val_bytes();

        ReadSequenceFile reader = new ReadSequenceFile();            
	reader.key_val_reader();

    }

    /*--------------------------------------------------------------------------------------------*/
}
