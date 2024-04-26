package org.dant;

import jakarta.annotation.Nullable;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.dant.model.*;
import org.jboss.resteasy.reactive.RestPath;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Path("/slave")
public class Slave {

    @POST
    @Path("/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void createTable(@RestPath String tableName, List<Column> listColumns) {
        if (DataBase.get().containsKey(tableName)) {
            System.out.println( "Table already exists with name : "+tableName);
            return;
        }
        if( listColumns.isEmpty() ) {
            System.out.println("Columns are empty");
            return;
        }
        new Table(tableName, listColumns);
        System.out.println("Table created successfully");
    }

    @POST
    @Path("/select")
    @Consumes(MediaType.APPLICATION_JSON)
    public List<List<Object>> getContent(SelectMethod selectMethod) {
        System.out.println("Select "+selectMethod.getSELECT()+", FROM "+selectMethod.getFROM());
        return DataBase.get().get(selectMethod.getFROM()).select(selectMethod);
    }

    @POST
    @Path("/insertOneRow/{name}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void insertOneRow(@RestPath String name, List<Object> args) {
        Table table = DataBase.get().get(name);
        if(table == null)
            throw new NotFoundException("La table avec le nom " + name + " n'a pas été trouvée.");
        if(args.size() != table.getColumns().size())
            throw new NotFoundException("Nombre d'argument incorrect.");
        table.addRow(args);
    }

    @POST
    @Path("/insertRows/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public void insertRows(@RestPath String tableName, List<List<Object>> listArgs) {
        Table table = DataBase.get().get(tableName);

        if(table == null)
            throw new NotFoundException("La table avec le nom " + tableName + " n'a pas été trouvée.");
        if( !listArgs.stream().allMatch( list -> list.size() == table.getColumns().size() ) )
            throw new NotFoundException("Nombre d'arguments invalide dans l'une des lignes.");

        table.addAllRows(listArgs.stream().map( list -> table.castRow(list)).collect(Collectors.toList()));
        System.out.println(listArgs.size()+" rows added !");
    }

    @POST
    @Path("/parquet/fillTable/{tableName}/{position}")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public void readParquet(@RestPath String tableName, InputStream inputStream, @RestPath int position) throws IOException {
        Configuration conf = new Configuration();
        Table table = DataBase.get().get(tableName);
        if (table == null)
            return;
        FileSystem fs = null;
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path("temp.parquet");
        try {
            fs = FileSystem.get(conf);
            System.out.println("Creation fichier parquet");
            try (FSDataOutputStream outputStream = fs.create(path)) {
                IOUtils.copy(inputStream, outputStream);
            }
            System.out.println("fin création");
        } catch (IOException e) {
            System.out.println("Erreur en recopiant le fichier parquet lu");
        };
        try (ParquetFileReader parquetFileReader = new ParquetFileReader(HadoopInputFile.fromPath(path, new Configuration()), ParquetReadOptions.builder().build())) {
            ParquetMetadata footer = parquetFileReader.getFooter();
            MessageType schema = footer.getFileMetaData().getSchema();
            PageReadStore pages;

            while ((pages = parquetFileReader.readNextRowGroup()) != null) {
                long rows = 300000;//pages.getRowCount();
                RecordReader<Group> recordReader = new ColumnIOFactory().getColumnIO(schema).getRecordReader(pages, new GroupRecordConverter(schema));
                for (long i=0; i<position*(rows/3); i++) {
                    recordReader.read();
                }
                ExecutorService executorService = Executors.newFixedThreadPool(3);
                for(long row=position*(rows/3); row<(position+1)*(rows/3); row++) {
                    SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    executorService.submit( ()-> table.addRowFromSimpleGroup(simpleGroup));
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (fs != null)
                    fs.delete(path, false);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("FINI !");
    }

}