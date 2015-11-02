package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;
import static com.mongodb.client.model.Sorts.descending;
import static com.mongodb.client.model.Sorts.orderBy;

/**
 * Created by Jorge on 02/11/2015.
 */
public class Homework_3_1 {

    public static void main(String [] args){
        // Conexión al servidor.
        MongoClient client = new MongoClient( new ServerAddress("localhost", 27017) );
        // Conexion a la BBDD.
        MongoDatabase database = client.getDatabase("school");
        // Recuperamos la coleccion de grades
        MongoCollection<Document> students = database.getCollection("students");

        Mongo mongo = new Mongo("localhost", 27017);
        DB db = mongo.getDB("school");
        DBCollection coleccion = db.getCollection("students");
        // Establecemos el filtro
        //Bson filter = and(eq("type", "homework"), eq("student_id", 199));
        Bson filter = eq("scores.type", "homework");
        // Establecemos el orden
        Bson order = new Document("name",1);
        Bson orderby = orderBy(ascending("name"), descending("score"));


        List<Document> greaterThan65 = students.find(filter).sort(orderby).into(new ArrayList<Document>());

        for(Document cur : greaterThan65){
            System.out.println(cur.toJson());
            Integer idEstudiante = cur.getInteger("_id");

            //BasicDBList lista =  (BasicDBList)cur.get("scores");
            ArrayList<String> lista = (ArrayList<String>)cur.get("scores");
            ArrayList<Double> scoresHomework = new ArrayList<Double>();
            for( Object elementoLista : lista  ) {

                //LinkedHashMap<String, String> elemento = (LinkedHashMap<String, String>)elementoLista;
                Document elemento = (Document)elementoLista;
                if(elemento.getString("type").equalsIgnoreCase("homework")){
                    //System.out.println(elemento.toString());
                    scoresHomework.add(elemento.getDouble("score"));
                }



            }
            //LinkedHashMap<String, String> lista = (LinkedHashMap<String, String>)cur.get("scores");
            Double scoreInferior = null;
            if(scoresHomework.get(0)> scoresHomework.get(1)) {
                scoreInferior = scoresHomework.get(1);
            }else if(scoresHomework.get(0)<scoresHomework.get(1)){
                scoreInferior = scoresHomework.get(0);
            }
            System.out.println(scoreInferior);


            BasicDBObject match = new BasicDBObject();
            match.put( "_id", idEstudiante );

            BasicDBObject addressSpec = new BasicDBObject();
            addressSpec.put( "type", "homework" );
            addressSpec.put( "score", scoreInferior);

            BasicDBObject update = new BasicDBObject();
            update.put( "$pull", new BasicDBObject( "scores", addressSpec ) );

            coleccion.update( match, update );




        }


    }
}
