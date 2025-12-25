package io.sustc.web;

import io.sustc.dto.AuthInfo;
import io.sustc.dto.PageResult;
import io.sustc.dto.RecipeRecord;
import io.sustc.service.RecipeService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
@RestController
@RequestMapping("/recipes")
@RequiredArgsConstructor
public class RecipeController {

    private final RecipeService recipeService;

    @GetMapping("/{id}/name")
    public String getName(@PathVariable long id) {
        return recipeService.getNameFromID(id);
    }

    @GetMapping("/{id}")
    public RecipeRecord getById(@PathVariable long id) {
        return recipeService.getRecipeById(id);
    }

    @GetMapping("/search")
    public PageResult<RecipeRecord> search(
            @RequestParam(required = false) String keyword,
            @RequestParam(required = false) String category,
            @RequestParam(required = false) Double minRating,
            @RequestParam(defaultValue = "1") Integer page,
            @RequestParam(defaultValue = "20") Integer size,
            @RequestParam(required = false) String sort
    ) {
        return recipeService.searchRecipes(keyword, category, minRating, page, size, sort);
    }

    @PostMapping
    public long create(@RequestHeader HttpHeaders headers,
                       @RequestBody RecipeRecord dto) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        return recipeService.createRecipe(dto, auth);
    }

    @DeleteMapping("/{id}")
    public void delete(@RequestHeader HttpHeaders headers,
                       @PathVariable long id) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        recipeService.deleteRecipe(id, auth);
    }

    public static class UpdateTimesReq {
        public String cookTimeIso;
        public String prepTimeIso;
    }

    @PatchMapping("/{id}/times")
    public void updateTimes(@RequestHeader HttpHeaders headers,
                            @PathVariable long id,
                            @RequestBody UpdateTimesReq req) {
        AuthInfo auth = AuthUtil.fromHeaders(headers);
        recipeService.updateTimes(auth, id, req.cookTimeIso, req.prepTimeIso);
    }

    @GetMapping("/closest-calorie-pair")
    public Map<String, Object> closestCaloriePair() {
        return recipeService.getClosestCaloriePair();
    }

    @GetMapping("/top3-complex")
    public List<Map<String, Object>> top3Complex() {
        return recipeService.getTop3MostComplexRecipesByIngredients();
    }
}
