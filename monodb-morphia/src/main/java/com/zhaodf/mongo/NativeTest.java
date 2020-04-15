package com.zhaodf.mongo;

import com.mongodb.client.*;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.*;
import org.bson.conversions.Bson;

/**
 * 类：NativeTest
 * 使用原生的mongdb驱动包进行api使用介绍
 * @author zhaodf
 * @date 2020/3/30
 */
public class NativeTest {
    public static void main(String[] args){
        //1、使用MongoClients来创建MongoClient连接
        MongoClient mongoClient = MongoClients.create("mongodb://127.0.0.1:27017");
        FindIterable<Document> documentFindIterable = findMongoCollectionDocument(mongoClient,"testdb","t_member");
        for (Document document:documentFindIterable) {
            System.out.println(document);
        }

        Document document = new Document();
        document.append("name","hanxb").append("age",90).append("sex","female");
        insertDocument(mongoClient,"testdb","t_member",document);

        BsonDocument bsonDocument = new BsonDocument();
        bsonDocument.append("name",new BsonString("hanxb"));
        deleteDocument(mongoClient,"testdb","t_member",bsonDocument);

        //过滤条件
        Document filter = new Document();
        filter.append("name", "hanxb");

        //修改的内容：注意update文档里要包含"$set"字段
        Document update = new Document();
        update.append("$set", new Document("sex", "male"));
        updateDocument(mongoClient,"testdb","t_member",filter,update);
    }

    /***
     * 获取数据库
     * @param mongoClient
     * @param dataBaseName
     * @return
     */
    public static MongoDatabase getDatabase(MongoClient mongoClient,String dataBaseName){
        return mongoClient.getDatabase(dataBaseName);
    }

    /***
     * 创建集合
     * @param dataBaseName
     * @param collectionName
     */
    public static void createCollection(MongoClient mongoClient,String dataBaseName,String collectionName){
        getDatabase(mongoClient,dataBaseName).createCollection(collectionName);
    }

    /***
     * 通过集合名字获取集合
     * @param dataBaseName
     * @param collectionName
     * @return
     */
    public static MongoCollection<Document> getCollectionByName(MongoClient mongoClient, String dataBaseName, String collectionName){
        return getDatabase(mongoClient,dataBaseName).getCollection(collectionName);
    }

    /***
     * 通过集合获取文档
     * @param dataBaseName
     * @param collectionName
     * @return
     */
    public static FindIterable<Document> findMongoCollectionDocument(MongoClient mongoClient, String dataBaseName, String collectionName){
        return getDatabase(mongoClient,dataBaseName).getCollection(collectionName).find();
    }

    /***
     * 插入一条文档
     * @param dataBaseName
     * @param collectionName
     * @param document
     */
    public static void insertDocument(MongoClient mongoClient, String dataBaseName, String collectionName, Document document){
        getCollectionByName(mongoClient,dataBaseName,collectionName).insertOne(document);
    }

    /***
     * 删除一条文档
     * @param dataBaseName
     * @param collectionName
     * @return  DeleteResult
     */
    public static DeleteResult deleteDocument(MongoClient mongoClient,String dataBaseName, String collectionName, Bson bson){
        return getCollectionByName(mongoClient,dataBaseName,collectionName).deleteOne(bson);
    }

    /***
     * 修改一条文档
     * @param dataBaseName
     * @param collectionName
     * @return  UpdateResult
     */
    public static UpdateResult updateDocument(MongoClient mongoClient,String dataBaseName, String collectionName, Bson filter,Bson update){
        return getCollectionByName(mongoClient,dataBaseName,collectionName).updateOne(filter,update);
    }
}
