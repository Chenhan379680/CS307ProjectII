package io.sustc.web;

import io.sustc.dto.*;
import io.sustc.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/users")
@RequiredArgsConstructor
public class UserController {

    private final UserService userService;

    @PostMapping("/register")
    public long register(@RequestBody RegisterUserReq req) {
        return userService.register(req);
    }

    @PostMapping("/login")
    public long login(@RequestHeader HttpHeaders headers) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return userService.login(auth);
    }

    @DeleteMapping("/{userId}")
    public boolean deleteAccount(@RequestHeader HttpHeaders headers,
                                 @PathVariable long userId) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return userService.deleteAccount(auth, userId);
    }

    @PostMapping("/{followeeId}/follow")
    public boolean followToggle(@RequestHeader HttpHeaders headers,
                                @PathVariable long followeeId) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return userService.follow(auth, followeeId);
    }

    @GetMapping("/{userId}")
    public UserRecord getById(@PathVariable long userId) {
        return userService.getById(userId);
    }

    public static class UpdateProfileReq {
        public String gender;
        public Integer age;
    }

    @PatchMapping("/profile")
    public void updateProfile(@RequestHeader HttpHeaders headers,
                              @RequestBody UpdateProfileReq req) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        userService.updateProfile(auth, req.gender, req.age);
    }

    @GetMapping("/feed")
    public PageResult<FeedItem> feed(@RequestHeader HttpHeaders headers,
                                     @RequestParam int page,
                                     @RequestParam int size,
                                     @RequestParam(required = false) String category) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return userService.feed(auth, page, size, category);
    }

    @GetMapping("/highest-follow-ratio")
    public Map<String, Object> highestFollowRatio() {
        return userService.getUserWithHighestFollowRatio();
    }
}
