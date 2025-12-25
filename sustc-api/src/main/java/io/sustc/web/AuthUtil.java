package io.sustc.web;

import io.sustc.dto.AuthInfo;
import org.springframework.http.HttpHeaders;

public class AuthUtil {
    // 你们如果要求别的 header 名，只要改这里就行
    public static AuthInfo fromHeaders(HttpHeaders headers) {
        String idStr = headers.getFirst("author_id");
        String pwd   = headers.getFirst("password");

        AuthInfo auth = new AuthInfo();
        if (idStr != null) {
            try { auth.setAuthorId(Long.parseLong(idStr)); } catch (NumberFormatException ignored) {}
        }
        auth.setPassword(pwd);
        return auth;
    }
}
