package io.sustc.service.impl;

import io.sustc.dto.ReviewRecord;
import io.sustc.dto.UserRecord;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.DatabaseService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;

import javax.sql.DataSource;
import java.sql.*;
import java.util.*;

@Service
@Slf4j
public class DatabaseServiceImpl implements DatabaseService {

    @Autowired
    private DataSource dataSource;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Override
    public List<Integer> getGroupMembers() {
        return Arrays.asList(12412610, 12410808); // 替换为你的学号
    }

    @Override
    @Transactional
    public void importData(
            List<ReviewRecord> reviewRecords,
            List<UserRecord> userRecords,
            List<RecipeRecord> recipeRecords) {

        Connection conn = DataSourceUtils.getConnection(dataSource);
        try {
            createBasicTables();

            String userSQL = "INSERT INTO users(AuthorId, AuthorName, Gender, Age, Followers, Following, Password, IsDeleted) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (AuthorId) DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(userSQL)) {
                int i = 0;
                for (UserRecord user : userRecords) {
                    ps.setLong(1, user.getAuthorId());
                    ps.setString(2, user.getAuthorName());
                    ps.setString(3, user.getGender());
                    ps.setInt(4, user.getAge());
                    ps.setInt(5, user.getFollowers());
                    ps.setInt(6, user.getFollowing());
                    ps.setString(7, user.getPassword());
                    ps.setBoolean(8, user.isDeleted());
                    ps.addBatch();
                    if (++i % 2000 == 0) ps.executeBatch(); // 增大 Batch Size
                }
                ps.executeBatch();
            }

            String recipeSQL = "INSERT INTO recipes (RecipeId, Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, SaturatedFatContent, CholesterolContent, SodiumContent, CarbohydrateContent, FiberContent, SugarContent, ProteinContent, RecipeServings, RecipeYield) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT (RecipeId) DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(recipeSQL)) {
                int i = 0;
                for (RecipeRecord r : recipeRecords) {
                    ps.setLong(1, r.getRecipeId());
                    ps.setString(2, r.getName());
                    ps.setLong(3, r.getAuthorId());
                    ps.setString(4, r.getCookTime());
                    ps.setString(5, r.getPrepTime());
                    ps.setString(6, r.getTotalTime());
                    ps.setTimestamp(7, r.getDatePublished());
                    ps.setString(8, r.getDescription());
                    ps.setString(9, r.getRecipeCategory());
                    ps.setFloat(10, r.getAggregatedRating());
                    ps.setFloat(11, r.getReviewCount());
                    ps.setFloat(12, r.getCalories());
                    ps.setFloat(13, r.getFatContent());
                    ps.setFloat(14, r.getSaturatedFatContent());
                    ps.setFloat(15, r.getCholesterolContent());
                    ps.setFloat(16, r.getSodiumContent());
                    ps.setFloat(17, r.getCarbohydrateContent());
                    ps.setFloat(18, r.getFiberContent());
                    ps.setFloat(19, r.getSugarContent());
                    ps.setFloat(20, r.getProteinContent());
                    ps.setFloat(21, r.getRecipeServings());
                    ps.setString(22, r.getRecipeYield());
                    ps.addBatch();
                    if (++i % 2000 == 0) ps.executeBatch();
                }
                ps.executeBatch();

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT setval('recipes_recipeid_seq', (SELECT MAX(Recipeid) FROM recipes))");
                }
            }

            String reviewSQL = "INSERT INTO reviews (ReviewId, RecipeId, AuthorId, Rating, Review, DateSubmitted, DateModified, LikesCount) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, 0) ON CONFLICT (ReviewId) DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(reviewSQL)) {
                int i = 0;
                for (ReviewRecord r : reviewRecords) {
                    ps.setLong(1, r.getReviewId());
                    ps.setLong(2, r.getRecipeId());
                    ps.setLong(3, r.getAuthorId());
                    // 修正：数据库 Rating 是 INT，这里强转 int 避免 float 精度问题
                    ps.setInt(4, (int) r.getRating());
                    ps.setString(5, r.getReview());
                    ps.setTimestamp(6, r.getDateSubmitted());
                    ps.setTimestamp(7, r.getDateModified());
                    ps.addBatch();
                    if (++i % 2000 == 0) ps.executeBatch();
                }
                ps.executeBatch();

                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT setval('reviews_reviewid_seq', (SELECT MAX(ReviewId) FROM reviews))");
                }
            }

            String ingredientsSQL = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?) ON CONFLICT DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(ingredientsSQL)) {
                int i = 0;
                for (RecipeRecord r : recipeRecords) {
                    Set<String> uniqueIngredients = new HashSet<>(Arrays.asList(r.getRecipeIngredientParts()));
                    for (String ingredient : uniqueIngredients) {
                        ps.setLong(1, r.getRecipeId());
                        ps.setString(2, ingredient);
                        ps.addBatch();
                        if (++i % 2000 == 0) ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }


            String reviewlikeSQL = "INSERT INTO review_likes (ReviewId, AuthorId) " +
                    "SELECT ?, ? " +
                    "WHERE EXISTS (SELECT 1 FROM reviews WHERE ReviewId = ?) " +
                    "  AND EXISTS (SELECT 1 FROM users WHERE AuthorId = ?) " +
                    "ON CONFLICT DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(reviewlikeSQL)) {
                int i = 0;
                for (ReviewRecord r : reviewRecords) {
                    if (r.getLikes() != null && r.getLikes().length > 0) {
                        Set<Long> uniqueLikes = new HashSet<>();
                        for (long id : r.getLikes()) uniqueLikes.add(id);

                        for (Long likeAuthorId : uniqueLikes) {
                            ps.setLong(1, r.getReviewId());
                            ps.setLong(2, likeAuthorId);
                            ps.setLong(3, r.getReviewId());
                            ps.setLong(4, likeAuthorId);
                            ps.addBatch();
                            if (++i % 2000 == 0) ps.executeBatch();
                        }
                    }
                }
                ps.executeBatch();
            }


            try (Statement stmt = conn.createStatement()) {
                log.info("Updating LikesCount in bulk...");
                stmt.execute("""
                            UPDATE reviews r
                            SET LikesCount = s.cnt
                            FROM (
                                SELECT ReviewId, COUNT(*) as cnt
                                FROM review_likes
                                GROUP BY ReviewId
                            ) s
                            WHERE r.ReviewId = s.ReviewId
                        """);
            }

            String userfollowsSQL = "INSERT INTO user_follows (FollowerId, FollowingId) VALUES (?, ?) ON CONFLICT DO NOTHING";
            try (PreparedStatement ps = conn.prepareStatement(userfollowsSQL)) {
                int i = 0;
                for (UserRecord user : userRecords) {
                    if (user.getFollowingUsers() != null) {
                        for (long followingId : user.getFollowingUsers()) {
                            ps.setLong(1, user.getAuthorId());
                            ps.setLong(2, followingId);
                            ps.addBatch();
                            if (++i % 2000 == 0) ps.executeBatch();
                        }
                    }
                }
                ps.executeBatch();
            }

            log.info("Creating indexes and triggers...");
            createIndexesAndTriggers();

        } catch (SQLException e) {
            log.error("Import failed", e);
            throw new RuntimeException("Import failed", e);
        } finally {
            DataSourceUtils.releaseConnection(conn, dataSource);
        }
    }

    private void createBasicTables() {
        String[] sqls = {
                "CREATE TABLE IF NOT EXISTS users (" +
                        "AuthorId BIGINT PRIMARY KEY," +
                        " AuthorName VARCHAR(255)," +
                        " Gender VARCHAR(10)," +
                        " Age INTEGER," +
                        " Followers INTEGER DEFAULT 0," +
                        " Following INTEGER DEFAULT 0," +
                        " Password VARCHAR(255)," +
                        " IsDeleted BOOLEAN DEFAULT FALSE" +
                        ")",


                "CREATE TABLE IF NOT EXISTS recipes " +
                        "(RecipeId BIGSERIAL PRIMARY KEY," +
                        " Name VARCHAR(500)," +
                        " AuthorId BIGINT," +
                        " CookTime VARCHAR(50)," +
                        " PrepTime VARCHAR(50)," +
                        " TotalTime VARCHAR(50)," +
                        " DatePublished TIMESTAMP," +
                        " Description TEXT," +
                        " RecipeCategory VARCHAR(255)," +
                        " AggregatedRating DECIMAL(3,2)," +
                        " ReviewCount INTEGER DEFAULT 0," +
                        " Calories DECIMAL(10,2)," +
                        " FatContent DECIMAL(10,2)," +
                        " SaturatedFatContent DECIMAL(10,2)," +
                        " CholesterolContent DECIMAL(10,2)," +
                        " SodiumContent DECIMAL(10,2)," +
                        " CarbohydrateContent DECIMAL(10,2)," +
                        " FiberContent DECIMAL(10,2)," +
                        " SugarContent DECIMAL(10,2)," +
                        " ProteinContent DECIMAL(10,2)," +
                        " RecipeServings VARCHAR(100)," +
                        " RecipeYield VARCHAR(100)," +
                        " FOREIGN KEY (AuthorId) REFERENCES users(AuthorId))",


                "CREATE TABLE IF NOT EXISTS reviews " +
                        "(ReviewId BIGSERIAL PRIMARY KEY," +
                        " RecipeId BIGINT," +
                        " AuthorId BIGINT," +
                        " Rating INTEGER," +
                        " Review TEXT," +
                        " DateSubmitted TIMESTAMP," +
                        " DateModified TIMESTAMP," +
                        " LikesCount BIGINT DEFAULT 0," +
                        " FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE," +
                        " FOREIGN KEY (AuthorId) REFERENCES users(AuthorId))",


                "CREATE TABLE IF NOT EXISTS recipe_ingredients " +
                        "(RecipeId BIGINT," +
                        " IngredientPart VARCHAR(500)," +
                        " PRIMARY KEY (RecipeId, IngredientPart)," +
                        " FOREIGN KEY (RecipeId) REFERENCES recipes(RecipeId) ON DELETE CASCADE)",


                "CREATE TABLE IF NOT EXISTS review_likes " +
                        "(ReviewId BIGINT, AuthorId BIGINT," +
                        " PRIMARY KEY (ReviewId, AuthorId)," +
                        " FOREIGN KEY (ReviewId) REFERENCES reviews(ReviewId) ON DELETE CASCADE," +
                        " FOREIGN KEY (AuthorId) REFERENCES users(AuthorId))",


                "CREATE TABLE IF NOT EXISTS user_follows " +
                        "(FollowerId BIGINT, FollowingId BIGINT," +
                        " PRIMARY KEY (FollowerId, FollowingId)," +
                        " FOREIGN KEY (FollowerId) REFERENCES users(AuthorId)," +
                        " FOREIGN KEY (FollowingId) REFERENCES users(AuthorId)," +
                        " CHECK (FollowerId != FollowingId))"
        };
        for (String sql : sqls) jdbcTemplate.execute(sql);
    }

    private void createIndexesAndTriggers() {
        String[] sqls = {

                // =========================
                // 1) Indexes
                // =========================
                "CREATE INDEX IF NOT EXISTS idx_recipes_rating ON recipes(aggregatedrating)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_date ON recipes(datepublished)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_calories ON recipes(calories)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_name ON recipes(name)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_category ON recipes(recipecategory)",
                "CREATE INDEX IF NOT EXISTS idx_ingr_recipe_id ON recipe_ingredients(recipeid)",
                "CREATE INDEX IF NOT EXISTS idx_ingr_name ON recipe_ingredients(ingredientpart)",
                "CREATE INDEX IF NOT EXISTS idx_users_name ON users(authorname)",
                "CREATE INDEX IF NOT EXISTS idx_user_follows_follower ON user_follows(followerid)",
                "CREATE INDEX IF NOT EXISTS idx_user_follows_following ON user_follows(followingid)",
                "CREATE INDEX IF NOT EXISTS idx_recipes_author_date ON recipes(authorid, datepublished DESC, recipeid DESC)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipe_date ON reviews(recipeid, datemodified DESC, reviewid DESC)",
                "CREATE INDEX IF NOT EXISTS idx_reviews_recipe_likes ON reviews(recipeid, likescount DESC, datemodified DESC, reviewid DESC)",


                // =========================
                // 2) Trigger: maintain LikesCount
                // =========================
                """
            CREATE OR REPLACE FUNCTION update_like_count() RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    UPDATE reviews SET likescount = COALESCE(likescount, 0) + 1 WHERE reviewid = NEW.reviewid;
                    RETURN NEW;
                ELSIF (TG_OP = 'DELETE') THEN
                    UPDATE reviews SET likescount = GREATEST(COALESCE(likescount, 0) - 1, 0) WHERE reviewid = OLD.reviewid;
                    RETURN OLD;
                END IF;
                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """,
                """
            DROP TRIGGER IF EXISTS trg_update_like_count ON review_likes;
            CREATE TRIGGER trg_update_like_count
            AFTER INSERT OR DELETE ON review_likes
            FOR EACH ROW EXECUTE FUNCTION update_like_count();
            """,


                // =========================
                // 3) NEW Trigger: maintain followers/following counts by DB
                // =========================
                """
            CREATE OR REPLACE FUNCTION update_follow_counts() RETURNS TRIGGER AS $$
            BEGIN
                IF (TG_OP = 'INSERT') THEN
                    UPDATE users
                    SET following = COALESCE(following, 0) + 1
                    WHERE authorid = NEW.followerid;

                    UPDATE users
                    SET followers = COALESCE(followers, 0) + 1
                    WHERE authorid = NEW.followingid;

                    RETURN NEW;

                ELSIF (TG_OP = 'DELETE') THEN
                    UPDATE users
                    SET following = GREATEST(COALESCE(following, 0) - 1, 0)
                    WHERE authorid = OLD.followerid;

                    UPDATE users
                    SET followers = GREATEST(COALESCE(followers, 0) - 1, 0)
                    WHERE authorid = OLD.followingid;

                    RETURN OLD;
                END IF;

                RETURN NULL;
            END;
            $$ LANGUAGE plpgsql;
            """,
                """
            DROP TRIGGER IF EXISTS trg_update_follow_counts ON user_follows;
            CREATE TRIGGER trg_update_follow_counts
            AFTER INSERT OR DELETE ON user_follows
            FOR EACH ROW EXECUTE FUNCTION update_follow_counts();
            """,


                // =========================
                // 4) View: follow ratio (for highest follow ratio query)
                // =========================
                """
            CREATE OR REPLACE VIEW v_follow_ratio AS
            WITH user_followers AS (
                SELECT followingid, COUNT(*) AS followers_cnt
                FROM user_follows
                GROUP BY followingid
            ),
            user_followings AS (
                SELECT followerid, COUNT(*) AS following_cnt
                FROM user_follows
                GROUP BY followerid
            )
            SELECT
                u.authorid,
                u.authorname,
                (COALESCE(ufr.followers_cnt, 0)::float8 / ufg.following_cnt::float8) AS ratio
            FROM users u
            JOIN user_followings ufg ON u.authorid = ufg.followerid
            LEFT JOIN user_followers ufr ON u.authorid = ufr.followingid
            WHERE u.isdeleted = FALSE;
            """,


                // =========================
                // 5) Function skeleton: closest calorie pair
                //    (Complete function body inserted; call: SELECT * FROM get_closest_calorie_pair();
                // =========================
                """
            CREATE OR REPLACE FUNCTION get_closest_calorie_pair()
            RETURNS TABLE(
                "RecipeA" BIGINT,
                "RecipeB" BIGINT,
                "CaloriesA" FLOAT8,
                "CaloriesB" FLOAT8,
                "Difference" FLOAT8
            )
            LANGUAGE sql
            AS $$
                WITH SortedRecipes AS (
                    SELECT recipeid, calories
                    FROM recipes
                    WHERE calories IS NOT NULL
                    ORDER BY calories ASC
                ),
                AdjacentPairs AS (
                    SELECT
                        recipeid AS id1,
                        calories AS cal1,
                        LEAD(recipeid) OVER (ORDER BY calories ASC) AS id2,
                        LEAD(calories) OVER (ORDER BY calories ASC) AS cal2
                    FROM SortedRecipes
                )
                SELECT
                    CAST(LEAST(id1, id2) AS BIGINT) AS "RecipeA",
                    CAST(GREATEST(id1, id2) AS BIGINT) AS "RecipeB",
                    CAST((CASE WHEN id1 < id2 THEN cal1 ELSE cal2 END) AS FLOAT8) AS "CaloriesA",
                    CAST((CASE WHEN id1 < id2 THEN cal2 ELSE cal1 END) AS FLOAT8) AS "CaloriesB",
                    CAST(ABS(cal1 - cal2) AS FLOAT8) AS "Difference"
                FROM AdjacentPairs
                WHERE id2 IS NOT NULL
                ORDER BY
                    "Difference" ASC,
                    "RecipeA" ASC,
                    "RecipeB" ASC
                LIMIT 1;
            $$;
            """
        };

        for (String sql : sqls) {
            jdbcTemplate.execute(sql);
        }
    }


    @Override
    public void drop() {
        String sql = "DO $$ DECLARE r RECORD; BEGIN FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = 'public') LOOP EXECUTE 'DROP TABLE IF EXISTS ' || QUOTE_IDENT(r.tablename) || ' CASCADE'; END LOOP; END $$;";
        jdbcTemplate.execute(sql);
    }

    @Override
    public Integer sum(int a, int b) {
        return jdbcTemplate.queryForObject("SELECT ?+?", Integer.class, a, b);
    }
}