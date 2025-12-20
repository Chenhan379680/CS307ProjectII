package io.sustc.service.impl;

import io.sustc.dto.*;
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
import org.springframework.util.StringUtils;
import org.springframework.lang.Nullable;


import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class UserServiceImpl implements UserService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final RowMapper<UserRecord> userRecordRowMapper = new BeanPropertyRowMapper<>(UserRecord.class);

    private final RowMapper<FeedItem> feedItemRowMapper = (rs, rowNum) -> {
        FeedItem item = new FeedItem();
        item.setRecipeId(rs.getLong("recipeid"));
        item.setName(rs.getString("name"));
        item.setAuthorId(rs.getLong("authorid"));
        item.setAuthorName(rs.getString("authorname"));
        item.setAggregatedRating(rs.getDouble("aggregatedrating"));
        item.setReviewCount(rs.getInt("reviewcount"));
        Timestamp ts = rs.getTimestamp("DatePublished", Calendar.getInstance(TimeZone.getTimeZone("UTC")));
        if (ts != null) {
            item.setDatePublished(ts.toInstant());
        }
        return item;
    };

    @Override
    public long register(RegisterUserReq req) {
        if(req == null) {
            return -1L;
        }

        if(req.getName() == null || req.getName().isEmpty()) {
            return -1L;
        }

        if (req.getGender() == null || req.getGender() == RegisterUserReq.Gender.UNKNOWN) {
            return -1L;
        }

        Integer age = calculateAgeFromBirthday(req.getBirthday());
        if (age == null || age <= 0) {
            return -1;
        }

        try {
            String idSQL = "SELECT COALESCE(MAX(AuthorId), 0) + 1 FROM users";
            Long newAuthorId = jdbcTemplate.queryForObject(idSQL, Long.class);
            if (newAuthorId == null) {
                newAuthorId = 1L;
            }
            String gender = getGender(req.getGender());
            String insertSQL = "INSERT INTO users(authorid, authorname, gender, age, followers, following, password, isdeleted) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ";
            Object[] args = {
                    newAuthorId,
                    req.getName(),
                    gender,
                    age,
                    0,
                    0,
                    req.getPassword(),
                    false
            };
            jdbcTemplate.update(insertSQL, args);
            return newAuthorId;
        } catch (Exception e) {
            return -1L;
        }
    }

    @Override
    public long login(AuthInfo auth) {
        if (auth == null || auth.getAuthorId() <= 0 || auth.getPassword() == null || auth.getPassword().isEmpty()) {
            return -1L;
        }
        try {
            String selectSQL = "SELECT password, isdeleted FROM users WHERE authorid = ?";
            Map<String, Object> userData = jdbcTemplate.queryForMap(selectSQL, auth.getAuthorId());
            Boolean isDeleted = (Boolean) userData.get("isdeleted");
            if (isDeleted != null && isDeleted) {
                return -1L;
            }
            String dbPassword = (String) userData.get("password");
            if (dbPassword == null || !dbPassword.equals(auth.getPassword())) {
                return -1L;
            }
            return auth.getAuthorId();
        } catch (EmptyResultDataAccessException e) {
            return -1L;
        } catch (Exception e) {
            e.printStackTrace();
            return -1L;
        }
    }

    @Override
    public boolean deleteAccount(AuthInfo auth, long userId) {
        // 1. 验证操作者身份
        verifyAuth(auth);

        // 2. 权限校验
        if (auth.getAuthorId() != userId) {
            throw new SecurityException("Access denied: You can only delete your own account.");
        }

        // 3. 检查目标用户状态
        String statusSql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        Boolean isDeleted;
        try {
            isDeleted = jdbcTemplate.queryForObject(statusSql, Boolean.class, userId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            throw new IllegalArgumentException("Target user does not exist.");
        }

        if (Boolean.TRUE.equals(isDeleted)) {
            return false;
        }

        // ================== 核心修改开始 ==================

        // 4.1 获取需要更新计数器的用户列表
        // 找出 "我关注的人" (他们的粉丝数需要 -1)
        String findFollowingSql = "SELECT followingid FROM user_follows WHERE followerid = ?";
        List<Long> followingIds = jdbcTemplate.queryForList(findFollowingSql, Long.class, userId);

        // 找出 "关注我的人" (他们的关注数需要 -1)
        String findFollowersSql = "SELECT followerid FROM user_follows WHERE followingid = ?";
        List<Long> followerIds = jdbcTemplate.queryForList(findFollowersSql, Long.class, userId);

        // 4.2 批量更新 "我关注的人" 的粉丝数 (FollowerCount - 1)
        if (!followingIds.isEmpty()) {
            String updateFollowerCountSql = "UPDATE users SET followers = COALESCE(followers, 0) - 1 WHERE AuthorId = ?";
            jdbcTemplate.batchUpdate(updateFollowerCountSql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                    ps.setLong(1, followingIds.get(i));
                }
                @Override
                public int getBatchSize() {
                    return followingIds.size();
                }
            });
        }

        // 4.3 批量更新 "关注我的人" 的关注数 (FollowingCount - 1)
        if (!followerIds.isEmpty()) {
            String updateFollowingCountSql = "UPDATE users SET following = COALESCE(following, 0) - 1 WHERE AuthorId = ?";
            jdbcTemplate.batchUpdate(updateFollowingCountSql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(java.sql.PreparedStatement ps, int i) throws java.sql.SQLException {
                    ps.setLong(1, followerIds.get(i));
                }
                @Override
                public int getBatchSize() {
                    return followerIds.size();
                }
            });
        }

        // 5. 现在可以安全地清理关注关系表了
        String clearFollowsSql = "DELETE FROM user_follows WHERE followerid = ? OR followingid = ?";
        jdbcTemplate.update(clearFollowsSql, userId, userId);

        // 6. 执行用户软删除
        // 可以在此处顺便把被删除用户的 Follower/Following Count 归零，或者保留原样（取决于业务需求），通常保留原样即可，因为已标记删除
        String deleteUserSql = "UPDATE users SET IsDeleted = TRUE WHERE AuthorId = ?";
        jdbcTemplate.update(deleteUserSql, userId);

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        verifyAuth(auth);
        long authorId = auth.getAuthorId();
        if(authorId == followeeId) {
            throw new SecurityException("Access denied: You cannot follow your own account.");
        }
        if(login(auth) == -1L) {
            throw new SecurityException("Access denied: You cannot login your own account.");
        }
        String checkSQL = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        try {
            Boolean isDeleted = jdbcTemplate.queryForObject(checkSQL, Boolean.class, followeeId);
            if(isDeleted) {
                throw new SecurityException("Access denied: You cannot follow an account.");
            }
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            throw new SecurityException("Target user does not exist.");
        }
        try {
            String relationSql = "SELECT COUNT(*) FROM user_follows WHERE followerid = ? AND followingid = ?";
            Integer count = jdbcTemplate.queryForObject(relationSql, Integer.class, auth.getAuthorId(), followeeId);
            if (count > 0) {
                // A. 已关注 -> 执行取消关注 (Unfollow)
                String deleteSql = "DELETE FROM user_follows WHERE followerid = ? AND followingid = ?";
                int rows = jdbcTemplate.update(deleteSql, auth.getAuthorId(), followeeId);
                if (rows > 0) {
                    // A2. 更新我的关注数 (My Following - 1)
                    // COALESCE 确保如果原来是 NULL 则视为 0 (防止 NULL - 1 = NULL)
                    String updateMyCount = "UPDATE users SET following = COALESCE(following, 0) - 1 WHERE authorid = ?";
                    jdbcTemplate.update(updateMyCount, authorId);

                    // A3. 更新对方的粉丝数 (Target Follower - 1)
                    String updateTargetCount = "UPDATE users SET followers = COALESCE(followers, 0) - 1 WHERE authorid = ?";
                    jdbcTemplate.update(updateTargetCount, followeeId);

                    return false;
                }
                return false;
            } else {
                // B. 未关注 -> 执行关注 (Follow)
                String insertSql = "INSERT INTO user_follows (followerid, followingid) VALUES (?, ?)";
                int rows = jdbcTemplate.update(insertSql, auth.getAuthorId(), followeeId);
                if (rows > 0) {
                    // B2. 更新我的关注数 (My Following + 1)
                    String updateMyCount = "UPDATE users SET following = COALESCE(following, 0) + 1 WHERE authorid = ?";
                    jdbcTemplate.update(updateMyCount, authorId);

                    // B3. 更新对方的粉丝数 (Target Follower + 1)
                    String updateTargetCount = "UPDATE users SET followers = COALESCE(followers, 0) + 1 WHERE authorid = ?";
                    jdbcTemplate.update(updateTargetCount, followeeId);

                    return true;
                }
                return false;
            }
        } catch (org.springframework.dao.DataIntegrityViolationException e) {
            return false;
        }
    }


    @Override
    public UserRecord getById(long userId) {
        String selectSQL = "SELECT * FROM users WHERE AuthorId = ?";
        try {
            UserRecord record = jdbcTemplate.queryForObject(selectSQL, userRecordRowMapper, userId);
            if(record != null) {
                String followerSQL = "SELECT followerid FROM user_follows WHERE followingid = ?";
                String followingSQL = "SELECT followingid FROM user_follows WHERE followerid = ?";
                List<Long> followerUsersList = jdbcTemplate.queryForList(followerSQL, Long.class, userId);
                List<Long> followingUsersList = jdbcTemplate.queryForList(followingSQL, Long.class, userId);
                long[] followerUsers = followerUsersList.stream().mapToLong(Long::longValue).toArray();
                long[] followingUsers = followingUsersList.stream().mapToLong(Long::longValue).toArray();
                record.setFollowerUsers(followerUsers);
                record.setFollowingUsers(followingUsers);
            }
            return record;
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public void updateProfile(AuthInfo auth, String gender, Integer age) {
        // 1. 验证用户身份 (必须存在且活跃)
        verifyAuth(auth);

        // 2. 参数校验 (Validation)
        // Gender: 如果不为 null，必须是 "Male" 或 "Female"
        if (gender != null && !gender.equals("Male") && !gender.equals("Female")) {
            throw new IllegalArgumentException("Invalid gender. Must be 'Male' or 'Female'.");
        }

        // Age: 如果不为 null，必须是正整数
        if (age != null && age <= 0) {
            throw new IllegalArgumentException("Invalid age. Must be a positive integer.");
        }

        // 3. 如果两个参数都是 null，直接返回，不需要操作数据库
        if (gender == null && age == null) {
            return;
        }

        // 4. 动态构建 SQL
        StringBuilder sqlBuilder = new StringBuilder("UPDATE users SET ");
        List<Object> params = new ArrayList<>();
        boolean needComma = false;

        if (gender != null) {
            sqlBuilder.append("Gender = ?");
            params.add(gender);
            needComma = true;
        }

        if (age != null) {
            if (needComma) {
                sqlBuilder.append(", ");
            }
            sqlBuilder.append("Age = ?");
            params.add(age);
        }

        // 5. 添加 WHERE 条件，确保只更新当前用户
        sqlBuilder.append(" WHERE AuthorId = ?");
        params.add(auth.getAuthorId());

        // 6. 执行更新
        jdbcTemplate.update(sqlBuilder.toString(), params.toArray());
    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, @Nullable String category) {
        // 1. 验证用户身份 (必须存在且活跃)
        verifyAuth(auth);

        // 2. 分页参数自动修正 (Auto-adjust)
        // 规则：page 从 1 开始，size 限制在 1~200 之间
        if (page < 1) page = 1;
        if (size < 1) size = 1;
        if (size > 200) size = 200;
        int offset = (page - 1) * size;

        // 3. 准备查询参数
        List<Object> params = new ArrayList<>();
        long currentUserId = auth.getAuthorId();

        // 4. 构建 SQL 语句
        // 核心思路：通过 JOIN user_follows 表，只筛选 "我关注的人" (uf.followingid) 发的菜谱
        StringBuilder sqlBuilder = new StringBuilder();

        // Base SQL: 基础筛选条件
        // r.* 代表查询菜谱的所有字段
        // uf.followerid = ? 锁定 "粉丝是我" 的记录
        // r.IsDeleted = FALSE 排除已被软删除的菜谱
        String baseClause = "FROM recipes r " +
                "JOIN user_follows uf ON r.authorid = uf.followingid " +
                "JOIN users u ON r.authorid = u.authorid " +
                "WHERE uf.followerid = ? ";
        params.add(currentUserId);

        // Category 过滤 (Optional)
        if (category != null && !category.trim().isEmpty()) {
            baseClause += "AND r.RecipeCategory = ? ";
            params.add(category);
        }

        // 5. 先查询总数 (Total Count)
        Long total;
        try {
            String countSql = "SELECT COUNT(*) " + baseClause;
            total = jdbcTemplate.queryForObject(countSql, Long.class, params.toArray());

            // 如果没有关注任何人，或者关注的人没发过符合条件的菜谱，直接返回空
            if (total == 0L) {
                return new PageResult<>(new ArrayList<>(), page, size, 0L);
            }
        } catch (EmptyResultDataAccessException e) {
            return new PageResult<>(new ArrayList<>(), page, size, 0L);
        }


        // 6. 查询具体数据 (Data Query)
        // 排序规则：发布时间倒序 -> ID 倒序 (保证分页稳定性)
        String querySql = "SELECT r.* , u.authorname " + baseClause +
                "ORDER BY r.DatePublished DESC, r.RecipeId DESC " +
                "LIMIT ? OFFSET ?";

        params.add(size);
        params.add(offset);
        List<FeedItem> records = jdbcTemplate.query(querySql, feedItemRowMapper, params.toArray());
        return new PageResult<>(records, page, size, total);
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        // SQL 逻辑解释：
        // 1. user_followers: 统计每个用户的粉丝数 (作为被关注者 FollowingId)
        // 2. user_followings: 统计每个用户的关注数 (作为关注者 FollowerId)
        // 3. 主查询: 连接 users 表，计算比率，排序取第一

        String sql = """
        WITH user_followers AS (
            SELECT FollowingId, COUNT(*) as cnt 
            FROM user_follows 
            GROUP BY FollowingId
        ),
        user_followings AS (
            SELECT FollowerId, COUNT(*) as cnt 
            FROM user_follows 
            GROUP BY FollowerId
        )
        SELECT 
            u.AuthorId, 
            u.AuthorName,
            CAST(COALESCE(fer.cnt, 0) AS DOUBLE PRECISION) / fing.cnt AS ratio
        FROM users u
        JOIN user_followings fing ON u.AuthorId = fing.FollowerId -- 必须有关注数 (Inner Join 自动排除了 count=0)
        LEFT JOIN user_followers fer ON u.AuthorId = fer.FollowingId -- 粉丝数可以是 0 (Left Join)
        WHERE u.IsDeleted = FALSE
        ORDER BY ratio DESC, u.AuthorId ASC
        LIMIT 1
        """;

        try {
            return jdbcTemplate.queryForObject(sql, (rs, rowNum) -> {
                Map<String, Object> result = new HashMap<>();
                result.put("AuthorId", rs.getLong("AuthorId"));
                result.put("AuthorName", rs.getString("AuthorName"));
                result.put("Ratio", rs.getDouble("ratio"));
                return result;
            });
        } catch (EmptyResultDataAccessException e) {
            // 如果没有符合条件的用户（例如所有用户都没有关注任何人），返回 null
            return null;
        }
    }

    public Integer calculateAgeFromBirthday(String birthday) {
        if(birthday == null || birthday.isEmpty()) {
            return null;
        }
        try {
            java.time.LocalDate birthDate;
            birthDate = java.time.LocalDate.parse(birthday);
            java.time.LocalDate now = LocalDate.now();
            if(birthDate.isAfter(now)) {
                return -1;
            }
            return java.time.Period.between(birthDate, now).getYears();
        } catch (Exception e) {
            return null;
        }
    }

    public String getGender(RegisterUserReq.Gender gender) {
        if(gender == RegisterUserReq.Gender.MALE) {
            return "Male";
        }
        if(gender == RegisterUserReq.Gender.FEMALE) {
            return "Female";
        }
        return "Unknown";
    }

    @Override
    public void verifyAuth(AuthInfo auth) {
        long result = login(auth);
        if (result <= 0) {
            throw new SecurityException("Authentication failed: Invalid user ID or password.");
        }
    }
}