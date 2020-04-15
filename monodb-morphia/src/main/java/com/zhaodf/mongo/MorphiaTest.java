package com.zhaodf.mongo;

import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.WriteResult;
import com.zhaodf.mongo.model.Member;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Key;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;
import org.mongodb.morphia.query.UpdateResults;

import java.util.Iterator;
import java.util.List;

/**
 * 类：MorphiaTest
 *
 * @author zhaodf
 * @date 2020/3/30
 */
public class MorphiaTest {
    public static void main(String[] args){
        Morphia morphia = new Morphia();
        Datastore datastore = morphia.createDatastore(new MongoClient("127.0.0.1",27017),"testdb");

        //插入一条文档
        Member member = new Member();
        member.setName("zhaolx");
        member.setSex("female");
        String id = insert(datastore,member);
        System.out.println(id);

        //查询全部文档
        List<Member> members = getAll(datastore);
        for (Member mem:members) {
            System.out.println(mem.getName()+"|"+mem.getAge());
        }

        //根据名称查询文档，并且按照年龄排序
        List<Member> memberList = getByName(datastore,"zhaodf");
        for (Member mem:memberList) {
            System.out.println(mem.getName()+"----->|"+mem.getAge());
        }

        UpdateResults results = updateByName(datastore,"zhaodf",20);
        System.out.println("更新了几条:"+results.getUpdatedCount());

        //根据名称删除
//        WriteResult result = deleteByName(datastore,"zhaolx");
//        System.out.println("删除了几条："+result.getN());
    }

    /***
     * 插入一条记录
     * @param datastore
     * @param member
     * @return
     */
    public static String insert(Datastore datastore,Member member) {
        return (String)datastore.save(member).getId();
    }

    /***
     * 插入多条文档
     * @param datastore
     * @param members
     * @return
     */
    public static Iterator<Key<Member>> insert(Datastore datastore, List<Member> members) {
        return datastore.save(members).iterator();
    }

    /***
     * 查询所有文档
     * @param datastore
     * @return
     */
    public static List<Member> getAll(Datastore datastore) {
        Query<Member> query = datastore.createQuery(Member.class);
        return query.asList();
    }

    /***
     * name去重查询
     * @param datastore
     * @param name
     * @return
     */
    public static List<String> getDistinctName(Datastore datastore,String name) {
        DBCollection dbCollection = datastore.getCollection(Member.class);
        return (List<String>)dbCollection.distinct(name);
    }

    /***
     * 根据name查询，并且按年龄排序
     * @param datastore
     * @param name
     * @return
     */
    public static List<Member> getByName(Datastore datastore,String name) {
        Query<Member> query = datastore.createQuery(Member.class)
                .field("name").equal(name)
                .order("age");
        return query.asList();
    }

    /***
     * 根据名称删除
     * @param datastore
     * @param name
     */
    public static WriteResult deleteByName(Datastore datastore, String name) {
        Query<Member> query = datastore.createQuery(Member.class).filter("name =",name);
        return datastore.delete(query);
    }

    /***
     * 根据名称更新
     * @param datastore
     * @param name
     */
    public static UpdateResults updateByName(Datastore datastore, String name,int inc) {
        Query<Member> query = datastore.createQuery(Member.class).filter("name =",name);
        UpdateOperations<Member> updateOperations = datastore.createUpdateOperations(Member.class)
                .set("age", inc);
        UpdateResults results = datastore.update(query, updateOperations);
        return datastore.update(query,updateOperations);
    }
}
