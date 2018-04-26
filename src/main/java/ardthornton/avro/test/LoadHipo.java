/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ardthornton.avro.test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import org.jlab.io.hipo.HipoDataBank;
import org.jlab.io.hipo.HipoDataEvent;
import org.jlab.io.hipo.HipoDataSource;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jlab.clas.physics.LorentzVector;
import scala.runtime.AbstractFunction1;

/**
 *
 * @author adam
 */
public class LoadHipo {
    
    public static void main(String[] args) throws InterruptedException, IOException, AnalysisException {
        LoadHipo app = new LoadHipo();
        
        // avro setup
        Schema schema = new Schema.Parser().parse(new File("event.avsc"));
        String hipoFilename = "/home/adam/uni/jlab/data/clas_dis_mcdata.hipo";
        String outputFilename = "events.avro";
        
        // run if needed
        //app.writeAvroFile(schema, outputFilename, hipoFilename);
        //app.readAvroFile(schema, outputFilename);
                
        // read the avro file with spark and filter the events
        SparkSession spark = SparkSession.builder()
                .appName("HIPO Avro test")
                .master("local[*]").getOrCreate();
                
        Dataset<Row> df = spark
                .read()
                .format("com.databricks.spark.avro")
                .load(outputFilename);
        df.createOrReplaceTempView("events");
        
        Dataset<Row> goodEvents = spark.sql(""
                + "select "
                + "eventNumber "
                + "from "
                + "events "
                + "where pid in (2122, -211) "
                + "group by eventNumber, pid");
        goodEvents.createOrReplaceTempView("good_events");
        
        Dataset<Row> filteredEvents = spark.sql(""
                + "select "
                + "* "
                + "from events "
                + "where eventNumber in "
                + "(select eventNumber from good_events)");
        filteredEvents.createOrReplaceTempView("filtered_events");
        filteredEvents.show(50);
    }
    
    
    private void writeAvroFile(
            Schema schema,
            String outputFilename,
            String hipoFilename) throws IOException {

        File file = new File(outputFilename);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        dataFileWriter.create(schema, file);
        
        // hipo setup reader
        HipoDataSource reader = new org.jlab.io.hipo.HipoDataSource();
        reader.open(hipoFilename);

        // iterate events and write them to avro file
        int nevents = reader.getSize();
        for(int i = 0; i < nevents; i++){
            HipoDataEvent event = (HipoDataEvent) reader.gotoEvent(i);
            HipoDataBank bank = (HipoDataBank) event.getBank("mc::event");
            for(int k = 0; k < bank.rows();   k++){
                GenericRecord ed = new GenericData.Record(schema);
                int pid = bank.getInt("pid", k);
                int status = bank.getByte("status",k);
                ed.put("eventNumber", i);
                ed.put("pid", pid);
                ed.put("vx", bank.getFloat("vx",k));
                ed.put("vy", bank.getFloat("vy",k));
                ed.put("vz", bank.getFloat("vz",k));
                ed.put("px", bank.getFloat("px",k));
                ed.put("py", bank.getFloat("py",k));
                ed.put("pz", bank.getFloat("pz",k));
                ed.put("mass", bank.getFloat("mass", k));
                ed.put("status", status);
                dataFileWriter.append(ed);
            }
        }
        dataFileWriter.close();
        reader.close();
    }
    
    private void readAvroFile(
            Schema schema,
            String outputFilename) throws IOException {
        // now read back the avro file
        // Deserialize users from disk
        File file = new File(outputFilename);
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        GenericRecord event = null;
        for(int i = 0; i < 10; i++){
            event = dataFileReader.next(event);
            System.out.println(event);
        }        
    }
}
