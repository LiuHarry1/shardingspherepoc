package org.example;

import com.zaxxer.hikari.HikariDataSource;
import org.apache.shardingsphere.driver.api.ShardingSphereDataSourceFactory;

import org.apache.shardingsphere.infra.config.algorithm.AlgorithmConfiguration;
import org.apache.shardingsphere.sharding.api.config.ShardingRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.rule.ShardingTableRuleConfiguration;
import org.apache.shardingsphere.sharding.api.config.strategy.sharding.StandardShardingStrategyConfiguration;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

public class ShardingH2Example {

    public static void main(String[] args) throws Exception {
        createTables(); //  注意：用 DriverManager 直连底层 db 建表

        DataSource shardingDataSource = createDataSource();

        // 插入测试数据
        insertOrder(shardingDataSource, 1001, 1, "INIT");
        insertOrder(shardingDataSource, 1002, 2, "PAID");
        insertOrder(shardingDataSource, 1003, 3, "SHIPPED");

        // 查询测试数据
        queryOrders(shardingDataSource);
    }


    public static DataSource createDataSource() throws Exception {
        Map<String, DataSource> dataSourceMap = new HashMap<>();

        // 模拟两个库：ds0 和 ds1（分别用两个内存 H2 实例模拟）
        for (int i = 0; i < 2; i++) {
            HikariDataSource ds = new HikariDataSource();
            ds.setJdbcUrl("jdbc:h2:mem:ds" + i + ";DB_CLOSE_DELAY=-1;MODE=MySQL");
            ds.setUsername("sa");
            ds.setPassword("");
            dataSourceMap.put("ds" + i, ds);
        }

        // 分表规则配置
        ShardingTableRuleConfiguration orderTableRule = new ShardingTableRuleConfiguration(
                "t_order", "ds${0..1}.t_order_${0..1}");

        // 分库策略（user_id）
        orderTableRule.setDatabaseShardingStrategy(new StandardShardingStrategyConfiguration(
                "user_id", "dbSharding"));

        // 分表策略（order_id）
        orderTableRule.setTableShardingStrategy(new StandardShardingStrategyConfiguration(
                "order_id", "tableSharding"));

        ShardingRuleConfiguration shardingRuleConfig = new ShardingRuleConfiguration();
        shardingRuleConfig.getTables().add(orderTableRule);

        Properties dbProps = new Properties();
        dbProps.setProperty("algorithm-expression", "ds${user_id % 2}");
        shardingRuleConfig.getShardingAlgorithms().put("dbSharding",
                new AlgorithmConfiguration("INLINE", dbProps));

        Properties tableProps = new Properties();
        tableProps.setProperty("algorithm-expression", "t_order_${order_id % 2}");
        shardingRuleConfig.getShardingAlgorithms().put("tableSharding",
                new AlgorithmConfiguration("INLINE", tableProps));

        return ShardingSphereDataSourceFactory.createDataSource(dataSourceMap, List.of(shardingRuleConfig), new Properties());
    }

    public static void createTables() throws SQLException {
        for (int i = 0; i < 2; i++) {
            String dbName = "ds" + i;
            try (Connection conn = DriverManager.getConnection("jdbc:h2:mem:" + dbName + ";DB_CLOSE_DELAY=-1;MODE=MySQL", "sa", "")) {
                Statement stmt = conn.createStatement();
                for (int j = 0; j < 2; j++) {
                    String sql = String.format("""
                        CREATE TABLE IF NOT EXISTS t_order_%d (
                            order_id BIGINT PRIMARY KEY,
                            user_id INT,
                            status VARCHAR(50)
                        )
                """, j);
                    stmt.execute(sql);
                }
            }
        }
    }


    // 辅助方法用于手动连接底层 H2 数据源
    private static HikariDataSource getH2DataSource(String name) {
        HikariDataSource ds = new HikariDataSource();
        ds.setJdbcUrl("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1;MODE=MySQL");
        ds.setUsername("sa");
        ds.setPassword("");
        return ds;
    }


    public static void insertOrder(DataSource ds, long orderId, int userId, String status) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            PreparedStatement ps = conn.prepareStatement("INSERT INTO t_order (order_id, user_id, status) VALUES (?, ?, ?)");
            ps.setLong(1, orderId);
            ps.setInt(2, userId);
            ps.setString(3, status);
            ps.executeUpdate();
        }
    }

    public static void queryOrders(DataSource ds) throws SQLException {
        try (Connection conn = ds.getConnection()) {
            ResultSet rs = conn.createStatement().executeQuery("SELECT * FROM t_order");
            System.out.println("Query Results:");
            while (rs.next()) {
                System.out.printf("order_id: %d, user_id: %d, status: %s%n",
                        rs.getLong("order_id"),
                        rs.getInt("user_id"),
                        rs.getString("status"));
            }
        }
    }
}
