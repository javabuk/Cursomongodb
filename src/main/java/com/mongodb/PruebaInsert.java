package com.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.BSON;
import org.bson.Document;
import org.bson.conversions.Bson;


import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;



/**
 * Created by Jorge on 25/10/2015.
 */
public class PruebaInsert {

    public static void main(String [] args){

        System.out.println("HOLA");

        // Conexión al servidor.
        MongoClient client = new MongoClient( new ServerAddress("localhost", 27017) );
        // Conexion a la BBDD.
        MongoDatabase database = client.getDatabase("students");
        // Recuperamos la coleccion de grades
        MongoCollection<Document> grades = database.getCollection("grades");
        // Establecemos el filtro
        Bson filter = gte("score", 65);
        // Establecemos el orden
        Bson order = new Document("score",1);

        List<Document> greaterThan65 = grades.find(filter).sort(order).into(new ArrayList<Document>());

        for(Document cur : greaterThan65){
            System.out.println(cur.toJson());
        }





        System.out.println(grades.count());






    }
}
