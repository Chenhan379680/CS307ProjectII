package io.sustc.web;

import io.sustc.dto.*;
import io.sustc.service.RecipeService;
import io.sustc.service.ReviewService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.*;
@RestController
@RequiredArgsConstructor
public class ReviewController {

    private final ReviewService reviewService;

    public static class ReviewReq {
        public int rating;
        public String review;
    }

    @PostMapping("/recipes/{recipeId}/reviews")
    public long add(@RequestHeader HttpHeaders headers,
                    @PathVariable long recipeId,
                    @RequestBody ReviewReq req) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return reviewService.addReview(auth, recipeId, req.rating, req.review);
    }

    @PutMapping("/recipes/{recipeId}/reviews/{reviewId}")
    public void edit(@RequestHeader HttpHeaders headers,
                     @PathVariable long recipeId,
                     @PathVariable long reviewId,
                     @RequestBody ReviewReq req) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        reviewService.editReview(auth, recipeId, reviewId, req.rating, req.review);
    }

    @DeleteMapping("/recipes/{recipeId}/reviews/{reviewId}")
    public void delete(@RequestHeader HttpHeaders headers,
                       @PathVariable long recipeId,
                       @PathVariable long reviewId) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        reviewService.deleteReview(auth, recipeId, reviewId);
    }

    @PostMapping("/reviews/{reviewId}/like")
    public long like(@RequestHeader HttpHeaders headers,
                     @PathVariable long reviewId) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return reviewService.likeReview(auth, reviewId);
    }

    @PostMapping("/reviews/{reviewId}/unlike")
    public long unlike(@RequestHeader HttpHeaders headers,
                       @PathVariable long reviewId) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return reviewService.unlikeReview(auth, reviewId);
    }

    @GetMapping("/recipes/{recipeId}/reviews")
    public PageResult<ReviewRecord> list(@PathVariable long recipeId,
                                         @RequestParam int page,
                                         @RequestParam int size,
                                         @RequestParam(required = false) String sort) {
        if (sort == null) sort = "date_desc";
        return reviewService.listByRecipe(recipeId, page, size, sort);
    }

    @PostMapping("/recipes/{recipeId}/refresh-rating")
    public RecipeRecord refresh(@RequestHeader HttpHeaders headers,
                                @PathVariable long recipeId) {
        // 让 refresh 也保持一致的“需要鉴权”
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return reviewService.refreshRecipeAggregatedRating(recipeId);
    }
}
