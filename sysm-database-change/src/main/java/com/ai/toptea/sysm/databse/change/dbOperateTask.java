package com.ai.toptea.sysm.databse.change;

import com.ai.toptea.basic.util.SnowflakeIdWorker;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class dbOperateTask {

    private static ApplicationContext ctx = new ClassPathXmlApplicationContext("classpath*:META-INF/spring/sysm-database-operate.xml");
    private static JdbcTemplate oracleJDBCTemplate = (JdbcTemplate) ctx.getBean("oracleJdbcTemplate");

    public static void main(String[] args){
        /*long begin = System.currentTimeMillis();
        System.out.println("ModifyTableTask start"+begin);
        dbOperateTask modifyTable = new dbOperateTask();
        String db = "sysm_gz";
        if(args != null && args.length > 0){
            db = args[0];
        }else{
            db = "sysm_gz";
        }
        List<String> tables = modifyTable.getAllTables(db);
        modifyTable.addTableIndactor(tables,db);
        long time = System.currentTimeMillis()-begin;
        System.out.println("ModifyTableTask end==耗时"+time);*/

        List list = getKeys(2);
        System.out.println(list);

        /*String table ="sysm_cd_alarm_subscribe_scope";

        updateMdcid(table);*/
//        System.out.println("123".contains(null));

    }

    public List<String> getAllTables(String dbname){
        try {
            System.out.println(dbname);
//            String sql = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA=? and CREATE_OPTIONS != 'partitioned' and TABLE_NAME not like 'sysm_rd_alarm_detail' AND TABLE_NAME LIKE 'sysm_%'";
            String sql = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA=? and  TABLE_NAME like 'sysm_rd_alarm_his'";
//            String sql = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA=? and  TABLE_NAME like 'sysm_rd_hourly_statistic_byattrs'";
//            String sql = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA=? and  TABLE_NAME like 'sysm_rd_performance_fct'";
//            String sql = "select TABLE_NAME from information_schema.TABLES where TABLE_SCHEMA=? ";
            List<String> parms = new ArrayList<>();
            parms.add(dbname);
            List<String> tables = oracleJDBCTemplate.query(sql, parms.toArray(), new RowMapper<String>() {
                @Override
                public String mapRow(ResultSet resultSet, int i) throws SQLException {
                    String str_table = resultSet.getString("TABLE_NAME");
                    return str_table;
                }
            });
            return tables;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public void addTableIndactor(List<String> tables, String db){
        if(tables != null && tables.size() > 0){
            for(String table : tables){
                /*System.out.println("1=="+table);
                addIndactor(table);
                System.out.println("2=="+table);
                updatePrimaryKey(table,db);
                System.out.println("3=="+table);
                addTableIndex(table,db);
                System.out.println("4=="+table);
                deleteTableIndactor(table);
                System.out.println("5=="+table);
                modifyTableIndactor(table);*/
//                changeunique(table);
//                printPrimary(table);


            }
        }
    }

    public void addIndactor(String table){
        try {
            System.out.println("==addIndactor=="+table);
            String sql = "ALTER TABLE "+table+" ADD COLUMN (MDCID bigint(20), MDCTIME datetime)";
            oracleJDBCTemplate.update(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void modifyTableIndactor(String table){//增加新的主键
        try {
            System.out.println("=modifyTableIndactor=="+table);
            String name = "MDCID";
            if("sysm_rd_alarm_his".equals(table)){
                name = "MDCID,ALARM_FIRST_OCCUR_TIME";
            }
            String sql = "ALTER TABLE "+table+" ADD PRIMARY KEY("+name+")";
            oracleJDBCTemplate.update(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void deleteTableIndactor(String table){//删除原有主键
        try {
            System.out.println("=deleteTableIndactor="+table);
            String sql = "ALTER TABLE "+table+" DROP PRIMARY KEY";
            oracleJDBCTemplate.update(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void addTableIndex(String table, String db){//加索引
        try {
            System.out.println("==addTableIndex=="+table);
            List<String> primary = this.getPrimaryKey(table, db);
            if(primary == null){
                System.out.println("=primary is null="+table);
                return;
            }
            StringBuffer index = new StringBuffer();
            for(int i=0; i<primary.size(); i++){
                index.append(primary.get(i));
                if(i<primary.size()-1){
                    index.append(",");
                }
            }

            System.out.println("==addTableIndex=index="+index);
            String name = "UNI_" + table;
            String sql = "ALTER TABLE "+table+" ADD CONSTRAINT "+ name.toUpperCase() +" UNIQUE("+ index.toString().toUpperCase() +")";
            oracleJDBCTemplate.update(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void updatePrimaryKey(String table, String db){
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            List<String> primary = this.getPrimaryKey(table, db);
            System.out.println("=primary="+primary.toString());
            long begin = System.currentTimeMillis();
            if(primary == null){
                System.out.println("=primary is null="+table);
                return;
            }
            StringBuffer sql = new StringBuffer("SELECT  ");
            for(int i=0; i<primary.size(); i++){
                sql.append(primary.get(i));
                if(i<primary.size()-1){
                    sql.append(", ");
                }
            }
            sql.append("  from "+table+" WHERE MDCID IS NULL");
            System.out.println("=sql="+sql.toString());
            List<Map<String, Object>> list = new ArrayList<>();
            DataSource source = oracleJDBCTemplate.getDataSource();
            conn = source.getConnection();
            ps = conn.prepareStatement(sql.toString());
            rs = ps.executeQuery();
            JSONArray aj = new JSONArray();
            while(rs.next()){
                JSONObject obj = new JSONObject();
                for(int i=0; i<primary.size(); i++){
                    obj.put(primary.get(i), rs.getString(primary.get(i)));
                }
                aj.add(obj);
            }
            rs.close();
            ps.close();
            conn.close();

            long time = System.currentTimeMillis()-begin;
            System.out.println("select_user_time="+time);

            if(aj.size() > 0){
                List<Long> keys = getKeys(aj.size());
                StringBuffer updatesql = new StringBuffer("UPDATE "+table+" SET MDCID = ? WHERE ");
                for(int i=0;i<primary.size(); i++) {
                    updatesql.append(primary.get(i) + " = ? ");
                    if(i<primary.size()-1){
                        updatesql.append(" and ");
                    }
                }

                conn = source.getConnection();
                ps = conn.prepareStatement(updatesql.toString());
                conn.setAutoCommit(false);
                for(int i=0;i<aj.size(); i++) {
                    ps.setLong(1,keys.get(i));

                    JSONObject obj = aj.getJSONObject(i);
                    for(int j=0; j<primary.size(); j++){
                        ps.setString(j+2,obj.getString(primary.get(j)));
                    }
                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (rs != null)
                    rs.close();
                if (ps != null)
                    ps.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                System.out.println("数据库连接无法关闭。");
            }
        }
    }

    public static List<Long> getKeys(int n){
        return SnowflakeIdWorker.getBatchId(n);
    }

    private List<String> getPrimaryKey(String table, String db) {
        try {
            String sql = "select COLUMN_NAME from information_schema.columns where TABLE_NAME = '" + table + "'  and columns.COLUMN_KEY = 'PRI' and TABLE_SCHEMA = '"+db+"'";
            List<String> values = oracleJDBCTemplate.query(sql, new RowMapper<String>() {
                @Override
                public String mapRow(ResultSet resultSet, int i) throws SQLException {
                    return resultSet.getString("COLUMN_NAME");
                }
            });
            return values;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }


    public void changeunique(String table){//改唯一索引
        try {
            System.out.println("=changeunique="+table);
            String sql = "ALTER TABLE "+table+" RENAME INDEX  "+ table.toUpperCase() +"  to UNI_"+table.toUpperCase();
            oracleJDBCTemplate.update(sql);
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    public void printPrimary(String table, String db){//改唯一索引
        List<String> primary = this.getPrimaryKey(table, db);
        System.out.println(table+"==pri="+primary.toString());
    }


    public static List<String> getRuleMdcid(String table){//获取超长度id
        String sql = "select MDCID from "+ table +" t where CHAR_LENGTH(t.mdcid)>16";
        try {
            List<String> values = oracleJDBCTemplate.query(sql, new RowMapper<String>() {
                @Override
                public String mapRow(ResultSet resultSet, int i) throws SQLException {
                    return resultSet.getString("MDCID");
                }
            });
            return values;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    public static void updateMdcid(String table){//更新mdcid

        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            System.out.println("=updateMdcid="+table);

            String sql = "update "+ table +" set MDCID = ? where MDCID = ?";
            DataSource source = oracleJDBCTemplate.getDataSource();
            conn = source.getConnection();
            ps = conn.prepareStatement(sql);
            conn.setAutoCommit(false);


            List<String> id = getRuleMdcid(table);
            System.out.println("=getRuleMdcid="+id.size());
            if(id != null) {
                List<Long> keys = getKeys(id.size());
                for (int i = 0; i < id.size(); i++) {
                    ps.setLong(1, keys.get(i));
                    ps.setLong(2, Long.parseLong(id.get(i)));

                    ps.addBatch();
                }
                ps.executeBatch();
                conn.commit();
                conn.setAutoCommit(true);
                ps.close();
                conn.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            try {
                if (rs != null)
                    rs.close();
                if (ps != null)
                    ps.close();
                if (conn != null)
                    conn.close();
            } catch (SQLException e) {
                System.out.println("数据库连接无法关闭。");
            }
        }
    }

}
