param(
  [string]$BaseUrl  = "http://localhost:8080",

  # User1
  [string]$User1Name = "Tommy",
  [string]$User1Gender = "MALE",
  [string]$User1Birthday = "2004-01-01",
  [string]$User1Password = "123",

  # User2 (用于点赞等需要“别人”的操作)
  [string]$User2Name = "Alice",
  [string]$User2Gender = "FEMALE",
  [string]$User2Birthday = "2004-02-02",
  [string]$User2Password = "123",

  # 你想测试的固定 recipeId（确保存在）
  [long]$DemoRecipeId = 1
)

# 不要全局 Stop，否则一个用例失败就直接退出，答辩不稳
$ErrorActionPreference = "Continue"

# ---------------------------
# Test bookkeeping
# ---------------------------
$global:PassCount = 0
$global:FailCount = 0
$global:FailList  = New-Object System.Collections.Generic.List[string]

function Section($title) {
  Write-Host ""
  Write-Host "==================== $title ====================" -ForegroundColor Cyan
}

function Record-Pass([string]$msg) {
  $global:PassCount++
  Write-Host "[OK] $msg" -ForegroundColor Green
}

function Record-Fail([string]$msg) {
  $global:FailCount++
  $global:FailList.Add($msg) | Out-Null
  Write-Host "[FAIL] $msg" -ForegroundColor Red
}

function Invoke-CurlJson {
  param(
    [Parameter(Mandatory=$true)][string]$Method,
    [Parameter(Mandatory=$true)][string]$Url,
    [hashtable]$Headers = @{},
    [object]$Body = $null,
    [int[]]$Ok = @(200),

    # 预期失败（例如测试权限/参数校验），把某些 HTTP code 视为 PASS
    [int[]]$ExpectFail = @(),
    [string]$Note = ""
  )

  $tmpOut  = New-TemporaryFile
  $tmpBody = $null

  $args = @("-s","-S","-o",$tmpOut.FullName,"-w","%{http_code}","-X",$Method,$Url)

  foreach ($k in $Headers.Keys) {
    $args += @("-H", ($k + ": " + $Headers[$k]))
  }

  if ($null -ne $Body) {
    $json = ($Body | ConvertTo-Json -Compress -Depth 30)
    $tmpBody = New-TemporaryFile
    Set-Content -Path $tmpBody.FullName -Value $json -NoNewline -Encoding utf8
    $args += @("-H","Content-Type: application/json","--data-binary","@$($tmpBody.FullName)")
  }

  $httpCode = & curl.exe @args
  $raw = Get-Content $tmpOut.FullName -Raw

  Remove-Item $tmpOut.FullName -Force
  if ($tmpBody) { Remove-Item $tmpBody.FullName -Force }

  $httpCodeInt = 0
  try { $httpCodeInt = [int]$httpCode } catch { $httpCodeInt = 0 }

  # PASS: 正常 OK
  if ($Ok -contains $httpCodeInt) {
    Record-Pass "$Method $Url -> $httpCodeInt $Note"
    if ([string]::IsNullOrWhiteSpace($raw)) { return $null }
    try { return ($raw | ConvertFrom-Json) } catch { return $raw }
  }

  # PASS: 预期失败（负例）
  if ($ExpectFail -contains $httpCodeInt) {
    Record-Pass "$Method $Url -> $httpCodeInt (expected) $Note"
    if ($raw) { Write-Host $raw }
    return $null
  }

  # FAIL
  Record-Fail "$Method $Url -> $httpCodeInt $Note"
  if ($raw) { Write-Host $raw }
  return $null
}

function Register-And-Login {
  param(
    [string]$Name,
    [string]$Gender,
    [string]$Birthday,
    [string]$Password
  )

  $uidObj = Invoke-CurlJson -Method POST -Url "$BaseUrl/users/register" -Body @{
    name = $Name
    gender = $Gender
    birthday = $Birthday
    password = $Password
  } -Ok @(200) -Note "(register $Name)"

  if ($null -eq $uidObj) { return $null }

  $userId = [long]$uidObj
  Write-Host "UserId($Name) = $userId" -ForegroundColor Yellow

  $headers = @{
    "author_id" = "$userId"
    "password"  = "$Password"
  }

  Invoke-CurlJson -Method POST -Url "$BaseUrl/users/login" -Headers $headers -Ok @(200) -Note "(login $Name)" | Out-Null
  return @{
    userId = $userId
    headers = $headers
  }
}

# ------------------------------------------------------------
# 0) Basic connectivity
# ------------------------------------------------------------
Section "0) Basic connectivity"
Invoke-CurlJson -Method GET -Url "$BaseUrl/recipes/$DemoRecipeId" -Ok @(200) -Note "(demo recipe exists)" | Out-Null

# ------------------------------------------------------------
# 1) Create two users
# ------------------------------------------------------------
Section "1) User - register/login/getById (two users)"

$u1 = Register-And-Login -Name $User1Name -Gender $User1Gender -Birthday $User1Birthday -Password $User1Password
$u2 = Register-And-Login -Name $User2Name -Gender $User2Gender -Birthday $User2Birthday -Password $User2Password

if ($null -ne $u1) {
  Invoke-CurlJson -Method GET -Url "$BaseUrl/users/$($u1.userId)" -Ok @(200) -Note "(getById user1)" | Out-Null
}
if ($null -ne $u2) {
  Invoke-CurlJson -Method GET -Url "$BaseUrl/users/$($u2.userId)" -Ok @(200) -Note "(getById user2)" | Out-Null
}

# ------------------------------------------------------------
# 2) User: feed / highest-follow-ratio
# ------------------------------------------------------------
Section "2) User - feed / highest-follow-ratio"
if ($null -ne $u1) {
  Invoke-CurlJson -Method GET -Url "$BaseUrl/users/feed?page=1&size=5" -Headers $u1.headers -Ok @(200) -Note "(feed as user1)" | Out-Null
}
Invoke-CurlJson -Method GET -Url "$BaseUrl/users/highest-follow-ratio" -Ok @(200) -Note "(analytics)" | Out-Null

# ------------------------------------------------------------
# 3) Recipe: getName/getById/search + analytics
# ------------------------------------------------------------
Section "3) Recipe - getName/getById/search/analytics"
Invoke-CurlJson -Method GET -Url "$BaseUrl/recipes/$DemoRecipeId/name" -Ok @(200) | Out-Null
Invoke-CurlJson -Method GET -Url "$BaseUrl/recipes/$DemoRecipeId" -Ok @(200) | Out-Null

# 注意：你们的 service 可能要求 page/size 从 1 开始，你之前已验证用 page=1 OK
Invoke-CurlJson -Method GET -Url "$BaseUrl/recipes/search?page=1&size=5" -Ok @(200) -Note "(page starts at 1 in this impl)" | Out-Null

Invoke-CurlJson -Method GET -Url "$BaseUrl/recipes/closest-calorie-pair" -Ok @(200) | Out-Null
Invoke-CurlJson -Method GET -Url "$BaseUrl/recipes/top3-complex" -Ok @(200) | Out-Null

# ------------------------------------------------------------
# 4) Recipe: create -> patch times -> delete
# ------------------------------------------------------------
Section "4) Recipe - create/patch/delete"
$rid = $null
if ($null -ne $u1) {
  $newRecipeId = Invoke-CurlJson -Method POST -Url "$BaseUrl/recipes" -Headers $u1.headers -Body @{
    name = "API Test Recipe"
    description = "created by run-api-tests.ps1"
    recipeCategory = "Test"
    recipeIngredientParts = @("salt","water")
    cookTime = "PT10M"
    prepTime = "PT5M"
    totalTime = "PT15M"
    datePublished = 1700000000000
    calories = 100.0
  } -Ok @(200) -Note "(create as user1)"

  if ($null -ne $newRecipeId) {
    $rid = [long]$newRecipeId
    Write-Host "New RecipeId = $rid" -ForegroundColor Yellow

    Invoke-CurlJson -Method PATCH -Url "$BaseUrl/recipes/$rid/times" -Headers $u1.headers -Body @{
      cookTimeIso = "PT20M"
      prepTimeIso = "PT10M"
    } -Ok @(200) -Note "(patch times)" | Out-Null

    Invoke-CurlJson -Method DELETE -Url "$BaseUrl/recipes/$rid" -Headers $u1.headers -Ok @(200) -Note "(delete)" | Out-Null
  }
}

# ------------------------------------------------------------
# 5) Review: add/edit/list/like/unlike/delete + refresh-rating
# ------------------------------------------------------------
Section "5) Review - add/edit/list/like/unlike/delete/refresh"

$reviewId = $null
if ($null -ne $u1) {
  $reviewIdObj = Invoke-CurlJson -Method POST -Url "$BaseUrl/recipes/$DemoRecipeId/reviews" -Headers $u1.headers -Body @{
    rating = 5
    review = "great!"
  } -Ok @(200) -Note "(add review as user1)"

  if ($null -ne $reviewIdObj) {
    $reviewId = [long]$reviewIdObj
    Write-Host "ReviewId = $reviewId" -ForegroundColor Yellow

    Invoke-CurlJson -Method PUT -Url "$BaseUrl/recipes/$DemoRecipeId/reviews/$reviewId" -Headers $u1.headers -Body @{
      rating = 4
      review = "updated review"
    } -Ok @(200) -Note "(edit review as user1)" | Out-Null
  }
}

Invoke-CurlJson -Method GET -Url "$BaseUrl/recipes/$DemoRecipeId/reviews?page=1&size=5" -Ok @(200) -Note "(list reviews)" | Out-Null

# 正例：用 user2 去 like/unlike user1 的 review，应该 200
if ($null -ne $reviewId -and $null -ne $u2) {
  Invoke-CurlJson -Method POST -Url "$BaseUrl/reviews/$reviewId/like" -Headers $u2.headers -Ok @(200) -Note "(like by user2)" | Out-Null
  Invoke-CurlJson -Method POST -Url "$BaseUrl/reviews/$reviewId/unlike" -Headers $u2.headers -Ok @(200) -Note "(unlike by user2)" | Out-Null
}

if ($null -ne $reviewId -and $null -ne $u1) {
  Invoke-CurlJson -Method DELETE -Url "$BaseUrl/recipes/$DemoRecipeId/reviews/$reviewId" -Headers $u1.headers -Ok @(200) -Note "(delete review as user1)" | Out-Null
}

Invoke-CurlJson -Method POST -Url "$BaseUrl/recipes/$DemoRecipeId/refresh-rating" -Ok @(200) -Note "(refresh rating)" | Out-Null

# ------------------------------------------------------------
# Summary
# ------------------------------------------------------------
Write-Host ""
Write-Host "==================== SUMMARY ====================" -ForegroundColor Cyan
Write-Host ("PASS = {0}, FAIL = {1}" -f $global:PassCount, $global:FailCount) -ForegroundColor Yellow

if ($global:FailCount -gt 0) {
  Write-Host "Failed cases:" -ForegroundColor Red
  $global:FailList | ForEach-Object { Write-Host (" - " + $_) -ForegroundColor Red }
  exit 1
} else {
  Write-Host "[DONE] ALL TESTS PASSED" -ForegroundColor Green
  exit 0
}
