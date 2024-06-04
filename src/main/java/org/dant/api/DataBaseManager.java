package org.dant.api;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import org.dant.commons.TypeDB;
import org.dant.model.Column;
import org.dant.model.DataBase;
import org.dant.model.Table;
import org.dant.rest.ForwardSlave1;
import org.dant.rest.ForwardSlave2;
import org.eclipse.microprofile.rest.client.inject.RestClient;
import org.jboss.resteasy.reactive.RestPath;

import java.util.List;
import java.util.Map;

@Path("/database")
public class DataBaseManager {

    @RestClient
    ForwardSlave1 forwardSlave1;
    @RestClient
    ForwardSlave2 forwardSlave2;

    @GET
    @Path("/showTables")
    public String getAllTables() {
        StringBuilder stringBuilder = new StringBuilder("Les Tables sont : \n");
        Map<String, Table> map = DataBase.get();
        for(String name : map.keySet()) {
            stringBuilder.append("\nTable ").append(name).append(" :\n");
            stringBuilder.append("Number of rows : ").append(map.get(name).getRows().size()).append("\n");
            List<Column> columns = map.get(name).getColumns();
            for( Column c : columns ) {
                stringBuilder.append("Colonne : ").append(c.getName()).append(", Type : ").append(c.getType()).append("\n");
            }
        }
        return stringBuilder.toString();
    }

    @POST
    @Path("/createTable/{tableName}")
    @Consumes(MediaType.APPLICATION_JSON)
    public String createTable(@RestPath String tableName, List<Column> listColumns) {
        if (DataBase.get().containsKey(tableName)) {
            return "Table already exists with name : "+tableName;
        }
        if( listColumns.isEmpty() )
            return "Columns are empty";
        forwardSlave1.createTable(tableName,listColumns);
        forwardSlave2.createTable(tableName,listColumns);
        new Table(tableName, listColumns);
        return "Table created successfully";
    }

    @POST
    @Path("/deleteColumn")
    @Consumes(MediaType.APPLICATION_JSON)
    public String deleteColumn(@QueryParam("tableName") String tableName, @QueryParam("nameColumn") String nameColumn) {
        Table table = DataBase.get().get(tableName);
        if(table == null)
            return ("La table '" + tableName + "' n'a pas été trouvée.");
        forwardSlave1.deleteColumn(tableName, nameColumn);
        forwardSlave2.deleteColumn(tableName, nameColumn);
        boolean deleted = table.deleteColumn(nameColumn);
        return deleted ? "Colonne '"+nameColumn+"' a bien été supprimé !" : "La colonne '"+nameColumn+"' n'a pas été trouvé.";
    }

    @POST
    @Path("/addColumn")
    @Consumes(MediaType.APPLICATION_JSON)
    public String addColumn(@QueryParam("tableName") String tableName, @QueryParam("nameColumn") String nameColumn,
                            @QueryParam("type") String type, @QueryParam("defaultValue") String defaultValue) {
        Table table = DataBase.get().get(tableName);
        if(table == null)
            return ("La table '" + tableName + "' n'a pas été trouvée.");
        if( !List.of(TypeDB.STRING, TypeDB.LONG, TypeDB.INT, TypeDB.DOUBLE, TypeDB.SHORT, TypeDB.BYTE).contains(type))
            return "Type invalide";
        Object val = null;
        if (defaultValue != null) {
            try {
                switch (type) {
                    case TypeDB.DOUBLE:
                        val = Double.parseDouble(defaultValue);
                        break;
                    case TypeDB.STRING:
                        val = defaultValue;
                        break;
                    case TypeDB.LONG:
                        val = Long.parseLong(defaultValue);
                        break;
                    case TypeDB.INT:
                        val = Integer.parseInt(defaultValue);
                        break;
                    case TypeDB.SHORT:
                        val = Short.parseShort(defaultValue);
                        break;
                    case TypeDB.BYTE:
                        val = Byte.parseByte(defaultValue);
                        break;
                    default:
                        return "Type non pris en charge";
                }
            } catch (ClassCastException | ArithmeticException e) {
                return "La valeur fournie ne correspond pas au type " + type;
            }
        }
        forwardSlave1.addColumn(tableName, nameColumn, type, defaultValue);
        forwardSlave2.addColumn(tableName, nameColumn, type, defaultValue);
        table.addNewColumn(nameColumn, type, val);
        return "Column added !";
    }

}
