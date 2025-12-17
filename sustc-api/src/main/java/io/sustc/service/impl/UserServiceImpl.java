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
        // 1. 验证操作者身份 (必须存在且活跃)
        verifyAuth(auth);

        // 2. 权限校验：只能删除自己的账户
        if (auth.getAuthorId() != userId) {
            throw new SecurityException("Access denied: You can only delete your own account.");
        }

        // 3. 检查目标用户是否存在及状态
        String statusSql = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        Boolean isDeleted;
        try {
            isDeleted = jdbcTemplate.queryForObject(statusSql, Boolean.class, userId);
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            // 目标用户不存在 -> IllegalArgumentException
            throw new IllegalArgumentException("Target user does not exist.");
        }

        // 4. 如果已经是删除状态，返回 false
        if (Boolean.TRUE.equals(isDeleted)) {
            return false;
        }

        // 5. 执行软删除 (Mark as inactive)
        String deleteUserSql = "UPDATE users SET IsDeleted = TRUE WHERE AuthorId = ?";
        jdbcTemplate.update(deleteUserSql, userId);

        // 6. 清理关注关系 (Remove follow relationships)
        // 需求：
        // a. 不再关注任何人 (follower_id = userId)
        // b. 没人再关注此人 (followee_id = userId)
        // 假设关注表名为 'follows'，字段为 'follower_id' 和 'followee_id' (请根据实际建表脚本调整表名/列名)
        String clearFollowsSql = "DELETE FROM user_follows WHERE followerid = ? OR followingid = ?";
        jdbcTemplate.update(clearFollowsSql, userId, userId);

        return true;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
        verifyAuth(auth);
        long authorId = auth.getAuthorId();
        if(authorId == followeeId) {
            throw new SecurityException("Access denied: You cannot follow your own account.");
        }
        String checkSQL = "SELECT IsDeleted FROM users WHERE AuthorId = ?";
        try {
            Boolean isDeleted = jdbcTemplate.queryForObject(checkSQL, Boolean.class, authorId);
            if(Boolean.TRUE.equals(isDeleted)) {
                return false;
            }
        } catch (org.springframework.dao.EmptyResultDataAccessException e) {
            return false;
        }
        String relationSql = "SELECT COUNT(*) FROM user_follows WHERE followerid = ? AND followingid = ?";
        Integer count = jdbcTemplate.queryForObject(relationSql, Integer.class, auth.getAuthorId(), followeeId);
        if (count != null && count > 0) {
            // A. 已关注 -> 执行取消关注 (Unfollow)
            String deleteSql = "DELETE FROM user_follows WHERE followerid = ? AND followingid = ?";
            int rows = jdbcTemplate.update(deleteSql, auth.getAuthorId(), followeeId);
            return rows > 0;
        } else {
            // B. 未关注 -> 执行关注 (Follow)
            String insertSql = "INSERT INTO user_follows (followerid, followingid) VALUES (?, ?)";
            int rows = jdbcTemplate.update(insertSql, auth.getAuthorId(), followeeId);
            return rows > 0;
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