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
        return false;
    }

    @Override
    public boolean follow(AuthInfo auth, long followeeId) {
     return false;
    }


    @Override
    public UserRecord getById(long userId) {
        return null;
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
}