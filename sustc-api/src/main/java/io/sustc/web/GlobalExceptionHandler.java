package io.sustc.web;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<?> badRequest(IllegalArgumentException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(Map.of("error", "bad_request", "message", e.getMessage()));
    }

    @ExceptionHandler(SecurityException.class)
    public ResponseEntity<?> unauthorized(SecurityException e) {
        // 也可以用 403，看你想表达“未登录”还是“无权限”
        return ResponseEntity.status(HttpStatus.UNAUTHORIZED)
                .body(Map.of("error", "unauthorized", "message", e.getMessage()));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<?> internal(Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(Map.of("error", "internal_error", "message", e.getMessage()));
    }
}
