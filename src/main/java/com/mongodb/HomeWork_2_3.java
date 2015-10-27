package com.mongodb;

/**
 * Created by Jorge on 26/10/2015.
 */
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.BSON;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;


import java.util.ArrayList;
import java.util.List;
import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Sorts.*;

public class HomeWork_2_3 {

    public static void main(String [] args){
        // Conexión al servidor.
        MongoClient client = new MongoClient( new ServerAddress("localhost", 27017) );
        // Conexion a la BBDD.
        MongoDatabase database = client.getDatabase("students");
        // Recuperamos la coleccion de grades
        MongoCollection<Document> grades = database.getCollection("grades");
        // Establecemos el filtro
        //Bson filter = and(eq("type", "homework"), eq("student_id", 199));
        Bson filter = eq("type", "homework");
        // Establecemos el orden
        Bson order = new Document("student_id",1);
        Bson orderby = orderBy(ascending("student_id"), descending("score"));
        //Bson orderby = ascending("student_id");


        List<Document> greaterThan65 = grades.find(filter).sort(orderby).into(new ArrayList<Document>());

        int identificadorEstudiante = 0;
        ObjectId identificadorRegistro = null;
        Double notaRegistro = null;

        /*for(Document cur : greaterThan65){

            Integer idEstudiante = cur.getInteger("student_id");
            ObjectId identificadorRegistroActual = cur.getObjectId("_id");
            if(identificadorEstudiante != idEstudiante.intValue()) {
                    System.out.println("Cambio de estudiante (" + identificadorEstudiante + ") eliminar "+ notaRegistro );
                    grades.deleteOne(eq("_id", identificadorRegistro));
            }
            System.out.println(cur.toJson());
            identificadorEstudiante = idEstudiante.intValue();
            identificadorRegistro = identificadorRegistroActual;
            notaRegistro = cur.getDouble("score");


        }*/
        int contador = 0;
        /*for(Document cur : greaterThan65){

            Integer idEstudiante = cur.getInteger("student_id");
            ObjectId identificadorRegistroActual = cur.getObjectId("_id");
            Double notaRegistroActual = cur.getDouble("score");
            if(contador == 1){
                grades.deleteOne(eq("_id", identificadorRegistroActual));
            }
            System.out.println(cur.toJson());
            identificadorEstudiante = idEstudiante.intValue();
            identificadorRegistro = identificadorRegistroActual;
            notaRegistro = cur.getDouble("score");
            contador++;

        }*/

        for(Document cur : greaterThan65){
            System.out.println(cur.toJson());
        }

        System.out.println(grades.count());


        Document usuario = new Document();
        usuario.append("_id", "jorge").append("password", "55").append("email","jorge@jorge.com");
        System.out.println(usuario.toJson());

    }
}
