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
       if(auth == null) {
           return -1L;
       }
       if(auth.getPassword() == null || auth.getPassword().isEmpty()) {
           return -1L;
       }
       try {
           String selectSQL = "SELECT password, isdeleted FROM users WHERE authorid = ?";
           Map<String, Object> map = jdbcTemplate.queryForMap(selectSQL, auth.getAuthorId());
           if((Boolean) map.get("isdeleted")) {
               return -1L;
           }
           if (map.get("password") == null || !map.get("password").toString().equals(auth.getPassword())) {
               return -1L;
           }
           return auth.getAuthorId();
       } catch (EmptyResultDataAccessException e) {
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

                    return true;
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
        verifyAuth(auth);
        if(login(auth) == -1L) {
            throw new SecurityException("Access denied: You cannot update your own account.");
        }

    }

    @Override
    public PageResult<FeedItem> feed(AuthInfo auth, int page, int size, String category) {
        return null;
    }

    @Override
    public Map<String, Object> getUserWithHighestFollowRatio() {
        return null;
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
}