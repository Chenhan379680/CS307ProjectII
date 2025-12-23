package io.sustc.service.impl;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import io.sustc.service.UserService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import java.sql.*;
import java.time.Duration;
import java.util.*;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private UserService userService;

    private final RowMapper<RecipeRecord> recipeRecordRowMapper = new BeanPropertyRowMapper<>(RecipeRecord.class);

    @Override
    public String getNameFromID(long id) {
        String sql = "SELECT name FROM recipes " +
                "WHERE recipeid = ? ";
        try {
            return jdbcTemplate.queryForObject(sql, String.class, id);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public RecipeRecord getRecipeById(long recipeId) {
        if (recipeId <= 0) {
            throw new IllegalArgumentException();
        }

        // 1. 修改 SQL：加入 LEFT JOIN 获取作者名字 (authorName)
        // 就像修复 searchRecipes 一样，这里也需要作者名
        String sql = """
            SELECT r.recipeid AS RecipeId,
                   r.name AS name,
                   r.authorid AS authorId,
                   u.authorname AS authorName,
                   r.cooktime AS cookTime,
                   r.preptime AS prepTime,
                   r.totaltime AS totalTime,
                   r.datepublished AS datePublished,
                   r.description,
                   r.recipecategory AS recipeCategory,
                   COALESCE(r.aggregatedRating, 0) as aggregatedRating,
                   r.reviewcount AS reviewCount,
                   r.calories,
                   r.fatcontent AS fatContent,
                   r.saturatedfatcontent AS saturatedFatContent,
                   r.cholesterolcontent AS  cholesterolContent,
                   r.sodiumcontent  AS sodiumContent,
                   r.carbohydratecontent AS carbohydrateContent,
                   r.fibercontent  AS fiberContent,
                   r.sugarcontent AS sugarContent,
                   r.proteincontent  AS proteinContent,
                   r.recipeservings AS recipeServings,
                   r.recipeyield AS recipeYield
            FROM recipes r
            LEFT JOIN users u ON r.authorid = u.authorid
            WHERE r.recipeid = ?
        """;

        try {
            RecipeRecord record = jdbcTemplate.queryForObject(sql, recipeRecordRowMapper, recipeId);
            if (record != null) {
                String ingredientSql = " SELECT ingredientpart FROM recipe_ingredients WHERE recipeid = ? ORDER BY LOWER(ingredientpart) ";
                String[] ingredients = jdbcTemplate.queryForList(ingredientSql, String.class, recipeId).toArray(new String[0]);

                record.setRecipeIngredientParts(ingredients);
            }
            return record;

        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }


    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating, Integer page, Integer size, String sort) {
        if (page == null || page < 1 || size == null || size <= 0) {
            throw new IllegalArgumentException("Page and size must be valid.");
        }

        String wherePart = " WHERE 1 = 1 ";
        List<Object> args = new ArrayList<>();

        if (StringUtils.hasText(keyword)) {
            String[] words = keyword.trim().split("\\s+");
            for (String word : words) {
                wherePart += " AND (LOWER(r.name) LIKE ? OR LOWER(r.description) LIKE ?) ";
                String token = "%" + word.toLowerCase() + "%";
                args.add(token);
                args.add(token);
            }
        }

        if (StringUtils.hasText(category)) {
            wherePart += " AND r.recipecategory = ? ";
            args.add(category);
        }

        if (minRating != null) {
            wherePart += " AND r.aggregatedrating >= ? ";
            args.add(minRating);
        }

        String countSQL = "SELECT COUNT(*) FROM recipes r " + wherePart;
        long total = jdbcTemplate.queryForObject(countSQL, Long.class, args.toArray());

        if (total == 0) {
            return new PageResult<>(new ArrayList<>(), page, size, 0L);
        }

        String sql = """
            SELECT r.*, u.authorname AS authorName
            FROM recipes r
            LEFT JOIN users u ON r.authorid = u.authorid
            """ + wherePart;

        if (StringUtils.hasText(sort)) {
            switch (sort) {
                case "rating_desc":
                    sql += " ORDER BY r.aggregatedrating DESC, r.recipeid DESC ";
                    break;
                case "date_desc":
                    sql += " ORDER BY r.datepublished DESC, r.recipeid DESC ";
                    break;
                case "calories_asc":
                    sql += " ORDER BY r.calories ASC, r.recipeid DESC ";
                    break;
                default:
                    sql += " ORDER BY r.recipeid DESC ";
                    break;
            }
        } else {
            sql += " ORDER BY r.recipeid DESC ";
        }

        sql += " LIMIT ? OFFSET ? ";
        args.add(size);
        args.add((page - 1) * size);

        // 10. 执行查询
        List<RecipeRecord> records = jdbcTemplate.query(
                sql,
                recipeRecordRowMapper,
                args.toArray()
        );

        if (!records.isEmpty()) {
            String ingredientSql = "SELECT ingredientpart FROM recipe_ingredients WHERE recipeid = ? ORDER BY LOWER(ingredientpart)";
            for (RecipeRecord record : records) {
                List<String> ingList = jdbcTemplate.queryForList(ingredientSql, String.class, record.getRecipeId());
                record.setRecipeIngredientParts(ingList.toArray(new String[0]));
            }
        }

        return new PageResult<>(records, page, size, total);
    }

    @Override
    @Transactional
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        // 1. 安全性校验 (Throws SecurityException)
        userService.verifyAuth(auth);

        // 2. 参数校验 (Throws IllegalArgumentException)
        if (dto.getName() == null || dto.getName().trim().isEmpty()) {
            throw new IllegalArgumentException("Recipe name cannot be null or empty.");
        }

        // 3. 准备 SQL - 显式列出字段，不包含自增的 RecipeId
        String sql = """
        INSERT INTO recipes (
            Name, AuthorId, CookTime, PrepTime, TotalTime, DatePublished, Description, 
            RecipeCategory, AggregatedRating, ReviewCount, Calories, FatContent, 
            SaturatedFatContent, CholesterolContent, SodiumContent, CarbohydrateContent, 
            FiberContent, SugarContent, ProteinContent, RecipeServings, RecipeYield
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """;

        // 4. 使用 KeyHolder 获取数据库自动生成的 ID
        KeyHolder keyHolder = new GeneratedKeyHolder();

        jdbcTemplate.update(connection -> {
            PreparedStatement ps = connection.prepareStatement(sql, new String[]{"recipeid"});

            // --- 基础信息 ---
            ps.setString(1, dto.getName());
            ps.setLong(2, auth.getAuthorId());
            ps.setString(3, dto.getCookTime());
            ps.setString(4, dto.getPrepTime());
            ps.setString(5, dto.getTotalTime());

            ps.setTimestamp(6, new Timestamp(System.currentTimeMillis()));

            ps.setString(7, dto.getDescription());
            ps.setString(8, dto.getRecipeCategory());

            // --- 初始状态 ---
            ps.setObject(9, null); // AggregatedRating
            ps.setInt(10, 0);      // ReviewCount

            // --- 营养成分 (允许为 NULL) ---
            ps.setObject(11, dto.getCalories());
            ps.setObject(12, dto.getFatContent());
            ps.setObject(13, dto.getSaturatedFatContent());
            ps.setObject(14, dto.getCholesterolContent());
            ps.setObject(15, dto.getSodiumContent());
            ps.setObject(16, dto.getCarbohydrateContent());
            ps.setObject(17, dto.getFiberContent());
            ps.setObject(18, dto.getSugarContent());
            ps.setObject(19, dto.getProteinContent());

            ps.setInt(20, dto.getRecipeServings());
            ps.setString(21, dto.getRecipeYield());

            return ps;
        }, keyHolder);

        Number key = keyHolder.getKey();
        if (key == null) {
            throw new RuntimeException("Failed to generate recipe ID.");
        }
        long newRecipeId = key.longValue();

        // 5. 插入配料 (去重 + 批量)
        if (dto.getRecipeIngredientParts() != null && dto.getRecipeIngredientParts().length > 0) {
            Set<String> uniqueIngredients = new HashSet<>();
            for (String p : dto.getRecipeIngredientParts()) {
                if (p != null && !p.trim().isEmpty()) {
                    uniqueIngredients.add(p.trim());
                }
            }

            if (!uniqueIngredients.isEmpty()) {
                String ingSql = "INSERT INTO recipe_ingredients (RecipeId, IngredientPart) VALUES (?, ?)";
                List<Object[]> batchArgs = new ArrayList<>();
                for (String ingredient : uniqueIngredients) {
                    batchArgs.add(new Object[]{newRecipeId, ingredient});
                }
                jdbcTemplate.batchUpdate(ingSql, batchArgs);
            }
        }

        return newRecipeId;
    }

    @Override
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        userService.verifyAuth(auth);

        String selectAuthSQL = "SELECT authorid FROM recipes WHERE recipeid = ?";
        long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(selectAuthSQL, Long.class, recipeId);
        } catch (EmptyResultDataAccessException e) {
            return;
        }

        if(authorId != auth.getAuthorId()) {
            throw new SecurityException();
        }
        String deleteSQL = """
                DELETE FROM recipes WHERE recipeid = ?;
                DELETE FROM recipe_ingredients WHERE recipeid = ?;
                """;
        jdbcTemplate.update(deleteSQL, recipeId, recipeId);
    }

    @Override
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        userService.verifyAuth(auth);

        // 1. 获取旧数据
        String selectSQL = "SELECT authorid, cooktime, preptime FROM recipes WHERE recipeid = ?";
        var recipeData = jdbcTemplate.query(selectSQL, (rs) -> {
            if (!rs.next()) return null;
            Map<String, Object> data = new HashMap<>();
            data.put("authorId", rs.getLong("authorid"));
            data.put("cookTime", rs.getString("cooktime"));
            data.put("prepTime", rs.getString("preptime"));
            return data;
        }, recipeId);

        if (recipeData == null) {
            throw new IllegalArgumentException("Recipe does not exist");
        }

        if ((long) recipeData.get("authorId") != auth.getAuthorId()) {
            throw new SecurityException("Only the recipe author can update times.");
        }
        String oldCookTimeStr = (String) recipeData.get("cookTime");
        String oldPrepTimeStr = (String) recipeData.get("prepTime");

        // 1. 确定 CookTime 的 Duration
        Duration cookDuration;
        if (cookTimeIso != null) {
            cookDuration = parseDurationStrict(cookTimeIso);
        } else {
            cookDuration = parseDurationLenient(oldCookTimeStr);
        }

        // 2. 确定 PrepTime 的 Duration
        Duration prepDuration;
        if (prepTimeIso != null) {
            prepDuration = parseDurationStrict(prepTimeIso);
        } else {
            prepDuration = parseDurationLenient(oldPrepTimeStr);
        }

        // 3. 安全计算 TotalTime
        Duration totalDuration = cookDuration.plus(prepDuration);
        String updateSQL = "UPDATE recipes SET cooktime = ?, preptime = ?, totaltime = ? WHERE recipeid = ?";

        jdbcTemplate.update(updateSQL,
                cookDuration.toString(),
                prepDuration.toString(),
                totalDuration.toString(),
                recipeId
        );
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        String sql = """
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
            -- 1. 确定 ID (A 是较小的 ID，B 是较大的 ID)
            CAST(LEAST(id1, id2) AS BIGINT) AS "RecipeA",
            CAST(GREATEST(id1, id2) AS BIGINT) AS "RecipeB",
            
            -- 2. 确定对应的 Calories
            CAST((CASE WHEN id1 < id2 THEN cal1 ELSE cal2 END) AS FLOAT8) AS "CaloriesA",
            CAST((CASE WHEN id1 < id2 THEN cal2 ELSE cal1 END) AS FLOAT8) AS "CaloriesB",
            
            -- 3. 计算差值 (转为 Java Double)
            CAST(ABS(cal1 - cal2) AS FLOAT8) AS "Difference"
        FROM AdjacentPairs
        WHERE id2 IS NOT NULL
        ORDER BY
            "Difference" ASC, -- 规则1: 差值最小
            "RecipeA" ASC,    -- 规则2: A 的 ID 较小
            "RecipeB" ASC     -- 规则3: B 的 ID 较小
        LIMIT 1
        """;

        try {
            return jdbcTemplate.queryForMap(sql);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public List<Map<String, Object>> getTop3MostComplexRecipesByIngredients() {
        String  sql = """
        SELECT
            r.recipeid AS "RecipeId",
            r.name AS "Name",
            CAST(COUNT(*) AS INTEGER) AS "IngredientCount"
        FROM recipe_ingredients ri
        JOIN recipes r ON ri.recipeid = r.recipeid
        GROUP BY r.recipeid, r.name
        ORDER BY 
            "IngredientCount" DESC ,
            "RecipeId" ASC
        LIMIT 3
        """;
        return jdbcTemplate.queryForList(sql);
    }


    private Duration parseDurationLenient(String isoString) {
        if (isoString == null || isoString.isBlank()) {
            return Duration.ZERO;
        }

        try {
            Duration d = Duration.parse(isoString);

            if (d.isNegative()) return Duration.ZERO;

            return d;
        } catch (java.time.format.DateTimeParseException e) {

            return Duration.ZERO;
        }
    }

    private Duration parseAndValidateDuration(String isoString) {
        try {
            Duration duration = Duration.parse(isoString);

            // 检查负数
            if (duration.isNegative()) {
                throw new IllegalArgumentException(" cannot be negative.");
            }
            return duration;

        } catch (java.time.format.DateTimeParseException e) {
            // 捕捉解析错误，包装成 IllegalArgumentException
            throw new IllegalArgumentException("Invalid ISO 8601 format");
        }
    }

    private Duration parseDurationStrict(String isoString) {
        if (isoString == null || isoString.isBlank()) {
            throw new IllegalArgumentException();
        }
        try {
            Duration d = Duration.parse(isoString);
            if (d.isNegative()) {
                throw new IllegalArgumentException();
            }
            return d;
        } catch (java.time.format.DateTimeParseException e) {
            throw new IllegalArgumentException();
        }
    }

}