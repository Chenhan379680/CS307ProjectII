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
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.sql.DataSource;
import java.sql.*;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class RecipeServiceImpl implements RecipeService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

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
            SELECT r.*, u.authorname AS authorName
            FROM recipes r
            LEFT JOIN users u ON r.authorid = u.authorid
            WHERE r.recipeid = ?
        """;

        try {
            // 执行主查询
            RecipeRecord record = jdbcTemplate.queryForObject(sql, recipeRecordRowMapper, recipeId);

            if (record != null) {
                // 2. 修复配料查询：使用 queryForList 获取多行字符串
                String ingredientSql = " SELECT ingredientpart FROM recipe_ingredients WHERE recipeid = ? ORDER BY LOWER(ingredientpart) ";
                String[] ingredients = jdbcTemplate.queryForList(ingredientSql, String.class, recipeId).toArray(new String[0]);

                // 3. 设置配料列表
                record.setRecipeIngredientParts(ingredients);
            }

            // 4. 在这里直接返回，或者把 record 定义提到 try 外面
            return record;

        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }


    @Override
    public PageResult<RecipeRecord> searchRecipes(String keyword, String category, Double minRating, Integer page, Integer size, String sort) {
        if (page == null || page < 1 || size == null || size <= 0) {
            throw new IllegalArgumentException();
        }
        StringBuilder wherePart = new StringBuilder(" WHERE 1 = 1 ");
        List<Object> args = new ArrayList<>();
        if(StringUtils.hasText(keyword)){
            wherePart.append(" AND (LOWER(r.name) LIKE ? OR LOWER(r.description) LIKE ?) ");
            String patten = "%" + keyword + "%";
            args.add(patten);
            args.add(patten);
        }

        if(StringUtils.hasText(category)){
            wherePart.append(" AND r.recipecategory = ? ");
            args.add(category);
        }

        if(minRating != null){
            wherePart.append(" AND r.aggregatedrating >= ? ");
            args.add(minRating);
        }

        String countSQL = "SELECT COUNT(*) FROM recipes r" +  wherePart.toString();
        Long total = jdbcTemplate.queryForObject(countSQL, Long.class, args.toArray());
        if(total == 0) {
            return new PageResult<>(new ArrayList<>(), page, size, 0L);
        }

        StringBuilder sql = new StringBuilder("""
                SELECT r.*, u.authorname AS authorName
                FROM recipes r
                LEFT JOIN users u ON r.authorid = u.authorid
                """).append(wherePart);

        if(StringUtils.hasText(sort)){
            switch (sort) {
                case "rating_desc" :
                    sql.append(" ORDER BY r.aggregatedrating DESC, r.recipeid DESC ");
                    break;
                case "date_desc" :
                    sql.append(" ORDER BY r.datepublished DESC, r.recipeid DESC ");
                    break;
                case "calories_asc" :
                    sql.append(" ORDER BY r.calories ASC, r.recipeid DESC ");
                    break;
                default:
                    break;
            }
        }

        sql.append(" LIMIT ? OFFSET ? ");
        args.add(size);
        args.add((page - 1) * size);
        List<RecipeRecord> records = jdbcTemplate.query(
                sql.toString(),
                recipeRecordRowMapper,
                args.toArray()
        );

        if (records != null && !records.isEmpty()) {
            String ingredientSql = " SELECT ingredientpart FROM recipe_ingredients WHERE recipeid = ? ORDER BY LOWER(ingredientpart) ";
            for (RecipeRecord record : records) {
                String[] ingredients = jdbcTemplate.queryForList(ingredientSql, String.class, record.getRecipeId()).toArray(new String[0]);
                record.setRecipeIngredientParts(ingredients);
            }
        }

        return new PageResult<>(records, page, size, total);
    }

    @Override
    public long createRecipe(RecipeRecord dto, AuthInfo auth) {
        verifyAuth(auth);

        if(dto.getName() == null || dto.getName().isEmpty()){
            throw new IllegalArgumentException();
        }

        String insertSQL = "INSERT INTO recipes (" +
                "RecipeId, " +
                "Name, " +
                "AuthorId, " +
                "CookTime, " +
                "PrepTime, " +
                "TotalTime, " +
                "DatePublished, " +
                "Description, " +
                "RecipeCategory, " +
                "AggregatedRating, " +
                "ReviewCount, " +
                "Calories, " +
                "FatContent, " +
                "SaturatedFatContent, " +
                "CholesterolContent, " +
                "SodiumContent, " +
                "CarbohydrateContent, " +
                "FiberContent, " +
                "SugarContent, " +
                "ProteinContent, " +
                "RecipeServings, " +
                "RecipeYield) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ";
        Object[] args = new Object[]{
                dto.getRecipeId(),
                dto.getName(),
                dto.getAuthorId(),
                dto.getCookTime(),
                dto.getPrepTime(),
                dto.getTotalTime(),
                dto.getDatePublished(),
                dto.getDescription(),
                dto.getRecipeCategory(),
                dto.getAggregatedRating(),
                dto.getReviewCount(),
                dto.getCalories(),
                dto.getFatContent(),
                dto.getSaturatedFatContent(),
                dto.getCholesterolContent(),
                dto.getSodiumContent(),
                dto.getCarbohydrateContent(),
                dto.getFiberContent(),
                dto.getSugarContent(),
                dto.getProteinContent(),
                dto.getRecipeServings(),
                dto.getRecipeYield(),
        };

        String ingredientsSQL = "INSERT INTO recipe_ingredients (" +
                "RecipeId, " +
                "IngredientPart) " +
                "VALUES (?, ?) ";
        try {
            jdbcTemplate.update(insertSQL, args);
            if(dto.getRecipeIngredientParts() != null){
                for(String ingredient : dto.getRecipeIngredientParts()) {
                    Object[] ingredientRow = new Object[2];
                    ingredientRow[0] = dto.getRecipeId();
                    ingredientRow[1] = ingredient;
                    jdbcTemplate.update(ingredientsSQL, ingredientRow);
                }
            }

        } catch (org.springframework.dao.DataAccessException e) {
            throw new IllegalArgumentException();
        }
        return dto.getRecipeId();
    }

    @Override
    public void deleteRecipe(long recipeId, AuthInfo auth) {
        verifyAuth(auth);

        String selcetAuthSQL = "SELECT authorid FROM recipes WHERE recipeid = ?";
        Long authorId;
        try {
            authorId = jdbcTemplate.queryForObject(selcetAuthSQL, Long.class, recipeId);
        } catch (EmptyResultDataAccessException e) {
            return;
        }

        if(authorId != auth.getAuthorId()) {
            throw new SecurityException();
        }

        String deleteSQL = "DELETE FROM recipe_ingredients WHERE recipeid = ?";
        jdbcTemplate.update(deleteSQL, recipeId);
    }

    @Override
    public void updateTimes(AuthInfo auth, long recipeId, String cookTimeIso, String prepTimeIso) {
        verifyAuth(auth);

        String selcetAuthSQL = """
            SELECT AuthorId, CookTime, PrepTime FROM recipes
            WHERE RecipeId = ?
        """;

        Map<String, Object> map;
        try {
            map = jdbcTemplate.queryForMap(selcetAuthSQL, recipeId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("No recipe");
        }

        long authorId = ((Number)map.get("AuthorId")).longValue();

        if(authorId != auth.getAuthorId()) {
            throw new SecurityException("Only author can update");
        }

        String oldCookTime = (String) map.get("cooktime");
        String oldPrepTime = (String) map.get("preptime");
        String newCookTime = (cookTimeIso != null) ? cookTimeIso : oldCookTime;
        String newPrepTime = (prepTimeIso != null) ? prepTimeIso : oldPrepTime;
        Duration cookDuration = parseAndValidateDuration(newCookTime);
        Duration prepDuration = parseAndValidateDuration(newPrepTime);
        Duration totalDuration = cookDuration.plus(prepDuration);
        String updateSQL = "UPDATE recipes " +
                "SET cooktime = ?, preptime = ?, totaltime = ? " +
                "WHERE recipeid = ?";
        jdbcTemplate.update(updateSQL,
                newCookTime,
                newPrepTime,
                totalDuration.toString(),
                recipeId);
    }

    @Override
    public Map<String, Object> getClosestCaloriePair() {
        // 假设表名为 recipes，列名为 id 和 calories
        // 使用 Postgres 的 Window Function (LEAD) 避免全表笛卡尔积，性能 O(N log N)
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
            
            -- 2. 确定对应的 Calories (注意要和上面的 ID 对应)
            CAST((CASE WHEN id1 < id2 THEN cal1 ELSE cal2 END) AS FLOAT8) AS "CaloriesA",
            CAST((CASE WHEN id1 < id2 THEN cal2 ELSE cal1 END) AS FLOAT8) AS "CaloriesB",
            
            -- 3. 计算差值 (转为 Java Double)
            CAST(ABS(cal1 - cal2) AS FLOAT8) AS "Difference"
        FROM AdjacentPairs
        WHERE id2 IS NOT NULL -- 过滤掉最后一行没有"下一行"的数据
        ORDER BY
            "Difference" ASC, -- 规则1: 差值最小
            "RecipeA" ASC,    -- 规则2: A 的 ID 较小
            "RecipeB" ASC     -- 规则3: B 的 ID 较小
        LIMIT 1
        """;

        try {
            return jdbcTemplate.queryForMap(sql);
        } catch (EmptyResultDataAccessException e) {
            // Corner Case: 不足两行数据时，Query 查不到结果，抛出此异常，按要求返回 null
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

    public void verifyAuth(AuthInfo auth) {
        if (auth == null) {
            throw new SecurityException();
        }
        String sql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";

        try {
            // queryForObject 查询单列
            Boolean isDeleted = jdbcTemplate.queryForObject(sql, Boolean.class, auth.getAuthorId());

            // 检查是否被删除
            // Boolean.TRUE.equals 可以安全处理 isDeleted 为 null 的情况 (视为未删除)
            if (Boolean.TRUE.equals(isDeleted)) {
                throw new SecurityException("User is deleted (inactive).");
            }

        } catch (EmptyResultDataAccessException e) {
            // 如果数据库里没有这个 AuthorId
            throw new SecurityException("User does not exist.");
        }
    }

    private Duration parseAndValidateDuration(String isoString) {
        if (isoString == null || isoString.trim().isEmpty()) {
            // 如果字段为空，视作 0 时间
            return Duration.ZERO;
        }

        try {
            // java.time.Duration.parse 完美支持 ISO 8601 (PnDTnHnMn.nS)
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

}