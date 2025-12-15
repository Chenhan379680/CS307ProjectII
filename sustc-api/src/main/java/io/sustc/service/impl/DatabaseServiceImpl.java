package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * It's important to mark your implementation class with {@link Service} annotation.
 * As long as the class is annotated and implements the corresponding interface, you can place it under any package.
 */
@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    /**
     * Getting a {@link DataSource} instance from the framework, whose connections are managed by HikariCP.
     * <p>
     * Marking a field with {@link Autowired} annotation enables our framework to automatically
     * provide you a well-configured instance of {@link DataSource}.
     * Learn more: <a href="https://www.baeldung.com/spring-dependency-injection">Dependency Injection</a>
     */
    @Autowired
    private DataSource dataSource;

    @Override
    public List<Integer> getGroupMembers() {
        //TODO: replace this with your own student IDs in your group
        return Arrays.asList(12412610, 12410808);
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords) {

        // ddl to create tables.
        createTables();

        // TODO: implement your import logic
        Connection conn = DataSourceUtils.getConnection(dataSource);
        try {
            String userSQL = "INSERT INTO users(" +
                    "AuthorId, " +
                    "AuthorName, " +
                    "Gender, " +
                    "Age, " +
                    "Followers, " +
                    "Following, " +
                    "Password, " +
                    "IsDeleted) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (AuthorId) DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(userSQL)) {
                for(int i = 0; i < userRecords.size(); i++) {
                    UserRecord userRecord = userRecords.get(i);
                    ps.setLong(1, userRecord.getAuthorId());
                    ps.setString(2, userRecord.getAuthorName());
                    ps.setString(3, userRecord.getGender());
                    ps.setInt(4, userRecord.getAge());
                    ps.setInt(5, userRecord.getFollowers());
                    ps.setInt(6, userRecord.getFollowing());
                    ps.setString(7, userRecord.getPassword());
                    ps.setBoolean(8, userRecord.isDeleted());
                    ps.addBatch();
                    if(i % 1000 == 0)
                        ps.executeBatch();
                }
                ps.executeBatch();
            }

            String recipeSQL = "INSERT INTO recipes (" +
                    "RecipeId," +
                    " Name," +
                    " AuthorId," +
                    " CookTime," +
                    " PrepTime," +
                    " TotalTime," +
                    " DatePublished," +
                    " Description," +
                    " RecipeCategory," +
                    " AggregatedRating," +
                    " ReviewCount," +
                    " Calories," +
                    " FatContent," +
                    " SaturatedFatContent," +
                    " CholesterolContent," +
                    " SodiumContent," +
                    " CarbohydrateContent," +
                    " FiberContent," +
                    " SugarContent," +
                    " ProteinContent," +
                    " RecipeServings," +
                    " RecipeYield, " +
                    " IsDeleted) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, false) " +
                    "ON CONFLICT (RecipeId) DO NOTHING";
            try(PreparedStatement ps = conn.prepareStatement(recipeSQL)) {
                for(int i = 0; i < recipeRecords.size(); i++) {
                    RecipeRecord recipeRecord = recipeRecords.get(i);
                    ps.setLong(1, recipeRecord.getRecipeId());
                    ps.setString(2, recipeRecord.getName());
                    ps.setLong(3, recipeRecord.getAuthorId());
                    ps.setString(4, recipeRecord.getCookTime());
                    ps.setString(5, recipeRecord.getPrepTime());
                    ps.setString(6, recipeRecord.getTotalTime());
                    ps.setTimestamp(7, recipeRecord.getDatePublished());
                    ps.setString(8, recipeRecord.getDescription());
                    ps.setString(9, recipeRecord.getRecipeCategory());
                    ps.setFloat(10, recipeRecord.getAggregatedRating());
                    ps.setFloat(11, recipeRecord.getReviewCount());
                    ps.setFloat(12, recipeRecord.getCalories());
                    ps.setFloat(13, recipeRecord.getFatContent());
                    ps.setFloat(14, recipeRecord.getSaturatedFatContent());
                    ps.setFloat(15, recipeRecord.getCholesterolContent());
                    ps.setFloat(16, recipeRecord.getSodiumContent());
                    ps.setFloat(17, recipeRecord.getCarbohydrateContent());
                    ps.setFloat(18, recipeRecord.getFiberContent());
                    ps.setFloat(19, recipeRecord.getSugarContent());
                    ps.setFloat(20, recipeRecord.getProteinContent());
                    ps.setFloat(21, recipeRecord.getRecipeServings());
                    ps.setString(22, recipeRecord.getRecipeYield());
                    ps.addBatch();
                    if(i % 1000 == 0)
                        ps.executeBatch();
                }
                ps.executeBatch();
            }

            String reviewSQL = "INSERT INTO reviews (" +
                    "ReviewId, " +
                    "RecipeId, " +
                    "AuthorId, " +
                    "Rating, " +
                    "Review, " +
                    "DateSubmitted, " +
                    "DateModified) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                    "ON CONFLICT (ReviewId) DO NOTHING";
            try(PreparedStatement ps = conn.prepareStatement(reviewSQL)) {
                for(int i = 0; i < reviewRecords.size(); i++) {
                    ReviewRecord reviewRecord = reviewRecords.get(i);
                    ps.setLong(1, reviewRecord.getReviewId());
                    ps.setLong(2, reviewRecord.getRecipeId());
                    ps.setLong(3, reviewRecord.getAuthorId());
                    ps.setFloat(4, reviewRecord.getRating());
                    ps.setString(5, reviewRecord.getReview());
                    ps.setTimestamp(6, reviewRecord.getDateSubmitted());
                    ps.setTimestamp(7, reviewRecord.getDateModified());
                    ps.addBatch();
                    if(i % 1000 == 0)
                        ps.executeBatch();
                }
                ps.executeBatch();
            }

            String ingredientsSQL = "INSERT INTO recipe_ingredients (" +
                    "RecipeId, " +
                    "IngredientPart) " +
                    "VALUES (?, ?) " +
                    "ON CONFLICT DO NOTHING";
            try(PreparedStatement ps = conn.prepareStatement(ingredientsSQL)) {
                int counter = 0;
                for(RecipeRecord recipeRecord : recipeRecords) {
                    Set<String> uniqueIngredients = new HashSet<>(Arrays.asList(recipeRecord.getRecipeIngredientParts()));
                    for(String ingredient : uniqueIngredients) {
                        ps.setLong(1, recipeRecord.getRecipeId());
                        ps.setString(2, ingredient);
                        ps.addBatch();
                        if(++counter % 1000 == 0)
                            ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }

            String reviewlikeSQL =  "INSERT INTO review_likes (ReviewId, AuthorId) " +
                    "SELECT ?, ? " +
                    "WHERE EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ?) " +
                    "  AND EXISTS (SELECT 1 FROM users WHERE AuthorId = ?) " +
                    "ON CONFLICT DO NOTHING";
            try(PreparedStatement ps = conn.prepareStatement(reviewlikeSQL)) {
                int counter = 0;
                for(ReviewRecord reviewRecord :  reviewRecords) {
                    if (reviewRecord.getLikes() != null && reviewRecord.getLikes().length > 0) {
                        Set<Long> uniqueLikes = new HashSet<>();
                        for (long like : reviewRecord.getLikes()) {
                            uniqueLikes.add(like);
                        }
                        for (long like : uniqueLikes) {
                            ps.setLong(1, reviewRecord.getReviewId());
                            ps.setLong(2, like);
                            ps.setLong(3, reviewRecord.getReviewId());
                            ps.setLong(4, like);
                            ps.addBatch();
                            if (++counter % 1000 == 0)
                                ps.executeBatch();
                        }
                    }
                }
                ps.executeBatch();
            }

            String userfollowsSQL = "INSERT INTO user_follows (" +
                    "FollowerId, " +
                    "FollowingId) " +
                    "VALUES (?, ?) " +
                    "ON CONFLICT DO NOTHING";
            try(PreparedStatement ps = conn.prepareStatement(userfollowsSQL)) {
                int counter = 0;
                for(UserRecord user : userRecords) {
                    if(user.getFollowingUsers() != null) {
                        for(long followingId : user.getFollowingUsers()) {
                            ps.setLong(1, user.getAuthorId());
                            ps.setLong(2, followingId);
                            ps.addBatch();
                            if(++counter % 1000 == 0)
                                ps.executeBatch();
                        }
                    }
                }
                ps.executeBatch();
            }

        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException("Import failed", e);
        } finally {
            DataSourceUtils.releaseConnection(conn, dataSource);
        }
    }


    private void createTables() {
        String[] createTableSQLs = {
                // 创建users表
                "CREATE TABLE IF NOT EXISTS users (" +
                        "    AuthorId BIGINT PRIMARY KEY, " +
                        "    AuthorName VARCHAR(255) NOT NULL, " +
                        "    Gender VARCHAR(10) CHECK (Gender IN ('Male', 'Female')), " +
                        "    Age INTEGER CHECK (Age > 0), " +
                        "    Followers INTEGER DEFAULT 0 CHECK (Followers >= 0), " +
                        "    Following INTEGER DEFAULT 0 CHECK (Following >= 0), " +
                        "    Password VARCHAR(255), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",

                // 创建recipes表
                "CREATE TABLE IF NOT EXISTS recipes (" +
                        "    RecipeId BIGINT PRIMARY KEY, " +
                        "    Name VARCHAR(500) NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    CookTime VARCHAR(50), " +
                        "    PrepTime VARCHAR(50), " +
                        "    TotalTime VARCHAR(50), " +
                        "    DatePublished TIMESTAMP, " +
                        "    Description TEXT, " +
                        "    RecipeCategory VARCHAR(255), " +
                        "    AggregatedRating DECIMAL(3,2) CHECK (AggregatedRating >= 0 AND AggregatedRating <= 5), " +
                        "    ReviewCount INTEGER DEFAULT 0 CHECK (ReviewCount >= 0), " +
                        "    Calories DECIMAL(10,2), " +
                        "    FatContent DECIMAL(10,2), " +
                        "    SaturatedFatContent DECIMAL(10,2), " +
                        "    CholesterolContent DECIMAL(10,2), " +
                        "    SodiumContent DECIMAL(10,2), " +
                        "    CarbohydrateContent DECIMAL(10,2), " +
                        "    FiberContent DECIMAL(10,2), " +
                        "    SugarContent DECIMAL(10,2), " +
                        "    ProteinContent DECIMAL(10,2), " +
                        "    RecipeServings VARCHAR(100), " +
                        "    RecipeYield VARCHAR(100), " +
                        "    IsDeleted BOOLEAN DEFAULT FALSE, " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建reviews表
                "CREATE TABLE IF NOT EXISTS reviews (" +
                        "    ReviewId BIGINT PRIMARY KEY, " +
                        "    RecipeId BIGINT NOT NULL, " +
                        "    AuthorId BIGINT NOT NULL, " +
                        "    Rating INTEGER, " +
                        "    Review TEXT, " +
                        "    DateSubmitted TIMESTAMP, " +
                        "    DateModified TIMESTAMP, " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建recipe_ingredients表
                "CREATE TABLE IF NOT EXISTS recipe_ingredients (" +
                        "    RecipeId BIGINT, " +
                        "    IngredientPart VARCHAR(500), " +
                        "    PRIMARY KEY (RecipeId, IngredientPart), " +
                        "    FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId)" +
                        ")",

                // 创建review_likes表
                "CREATE TABLE IF NOT EXISTS review_likes (" +
                        "    ReviewId BIGINT, " +
                        "    AuthorId BIGINT, " +
                        "    PRIMARY KEY (ReviewId, AuthorId), " +
                        "    FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId), " +
                        "    FOREIGN KEY (AuthorId) REFERENCES users(AuthorId)" +
                        ")",

                // 创建user_follows表
                "CREATE TABLE IF NOT EXISTS user_follows (" +
                        "    FollowerId BIGINT, " +
                        "    FollowingId BIGINT, " +
                        "    PRIMARY KEY (FollowerId, FollowingId), " +
                        "    FOREIGN KEY (FollowerId) REFERENCES users(AuthorId), " +
                        "    FOREIGN KEY (FollowingId) REFERENCES users(AuthorId), " +
                        "    CHECK (FollowerId != FollowingId)" +
                        ")"
        };

        for (String sql : createTableSQLs) {
            jdbcTemplate.execute(sql);
        }
    }



    /*
     * The following code is just a quick example of using jdbc datasource.
     * Practically, the code interacts with database is usually written in a DAO layer.
     *
     * Reference: [Data Access Object pattern](https://www.baeldung.com/java-dao-pattern)
     */

    @Override
    public void drop() {
        // You can use the default drop script provided by us in most cases,
        // but if it doesn't work properly, you may need to modify it.
        // This method will delete all the tables in the public schema.

        String sql = "DO $$\n" +
                "DECLARE\n" +
                "    tables CURSOR FOR\n" +
                "        SELECT tablename\n" +
                "        FROM pg_tables\n" +
                "        WHERE schemaname = 'public';\n" +
                "BEGIN\n" +
                "    FOR t IN tables\n" +
                "    LOOP\n" +
                "        EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(t.tablename) || ' CASCADE;';\n" +
                "    END LOOP;\n" +
                "END $$;\n";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer sum(int a, int b) {
        String sql = "SELECT ?+?";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setInt(1, a);
            stmt.setInt(2, b);
            log.info("SQL: {}", stmt);

            ResultSet rs = stmt.executeQuery();
            rs.next();
            return rs.getInt(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
