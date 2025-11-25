import random 
import json
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, ElementClickInterceptedException, NoSuchElementException
from bs4 import BeautifulSoup
from selenium_stealth import stealth
# ============================================
# Configuration
# ============================================
chrome_options = Options()
chrome_options.add_argument("--headless=new")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.binary_location = r"/home/mhai/Project DE/EXAM_DATA/Week5 + Week6/kafka/chrome.exe"
chrome_options.add_argument(
    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

driver = webdriver.Chrome(options=chrome_options)
stealth(driver,
        languages=["en-US", "en"],
        vendor="Google Inc.",
        platform="Win32",
        webgl_vendor="Intel Inc.",
        renderer="Intel Iris OpenGL Engine",
        fix_hairline=True,
)
# ============================================
# Crawl danh sách diễn viên , đạo diễn
# ============================================
def actors_list(actor) :
    director_list = [s.text.strip() for s in actor.select("li.ipc-metadata-list__item.ipc-metadata-list__item--align-end"
                                                    " div.ipc-metadata-list-item__content-container "
                                                    " ul.ipc-inline-list" 
                                                    " li.ipc-inline-list__item "
                                                    " a.ipc-metadata-list-item__list-content-item")]
    director = director_list[0]

    star_list = [s.text.strip() for s in actor.select("li.ipc-metadata-list__item.ipc-metadata-list__item--align-end.ipc-metadata-list-item--link"
                                                    " div.ipc-metadata-list-item__content-container "
                                                    " ul.ipc-inline-list" 
                                                    " li.ipc-inline-list__item "
                                                    " a.ipc-metadata-list-item__list-content-item")]
    stars = star_list[0:3]
    writers = [s for s in director_list[1:4] if s != star_list[0]]

    country_el = actor.select_one('li.ipc-metadata-list__item.ipc-metadata-list__item--align-end[data-testid="title-details-origin"]'
                                                    " div.ipc-metadata-list-item__content-container "
                                                    " ul.ipc-inline-list" 
                                                    " li.ipc-inline-list__item "
                                                    " a.ipc-metadata-list-item__list-content-item")
    country  = country_el.text.strip() if country_el else ""


    language_el = actor.select_one('li.ipc-metadata-list__item.ipc-metadata-list__item--align-end[data-testid="title-details-languages"]'
                                                    " div.ipc-metadata-list-item__content-container "
                                                    " ul.ipc-inline-list" 
                                                    " li.ipc-inline-list__item "
                                                    " a.ipc-metadata-list-item__list-content-item")
    language  = language_el.text.strip() if language_el else ""

    company_el = actor.select_one('li.ipc-metadata-list__item.ipc-metadata-list__item--align-end[data-testid="title-details-companies"]'
                                                    " div.ipc-metadata-list-item__content-container "
                                                    " ul.ipc-inline-list" 
                                                    " li.ipc-inline-list__item "
                                                    " a.ipc-metadata-list-item__list-content-item")
    company  = company_el.text.strip() if company_el else ""

    budget_el = actor.select_one('li.ipc-metadata-list__item.ipc-metadata-list__item--align-end[data-testid="title-boxoffice-budget"]'
                                                    " div.ipc-metadata-list-item__content-container "
                                                    " ul.ipc-inline-list" 
                                                    " li.ipc-inline-list__item "
                                                    " span.ipc-metadata-list-item__list-content-item.ipc-btn--not-interactable")
    budget  = budget_el.text.strip() if budget_el else ""

    gross_us_canada_el = actor.select_one('li.ipc-metadata-list__item.ipc-metadata-list__item--align-end[data-testid="title-boxoffice-grossdomestic"]'
                                                    " div.ipc-metadata-list-item__content-container "
                                                    " ul.ipc-inline-list" 
                                                    " li.ipc-inline-list__item "
                                                    " span.ipc-metadata-list-item__list-content-item.ipc-btn--not-interactable")
    gross_us_canada  = gross_us_canada_el.text.strip() if gross_us_canada_el else ""

    gross_worldwide_el = actor.select_one('li.ipc-metadata-list__item.ipc-metadata-list__item--align-end[data-testid="title-boxoffice-cumulativeworldwidegross"]'
                                                        " div.ipc-metadata-list-item__content-container "
                                                        " ul.ipc-inline-list" 
                                                        " li.ipc-inline-list__item "
                                                        " span.ipc-metadata-list-item__list-content-item.ipc-btn--not-interactable")
    gross_worldwide  = gross_worldwide_el.text.strip() if gross_worldwide_el else ""

    revenue = gross_worldwide if gross_worldwide is not None else gross_us_canada  

    plot_el = actor.select_one("span[data-testid='plot-l']")
    plot = plot_el.text.strip() if plot_el else ""

    poster_el = actor.select_one("img.ipc-image")
    poster = poster_el["src"] if poster_el else ""

    return director, writers, stars, country, language, company, budget, revenue , plot, poster

# ============================================
# Crawl danh sách bình luận 
# ============================================
def reviews_list(review , reviews , movie_id , num) :
    review_list = review.select("section.ipc-page-section article.sc-7ebcc14f-1.dtHbLR.user-review-item")
    for rv in review_list :
        review_id = f"R00{num}" 

        star_el = rv.select_one("span.ipc-rating-star--rating")
        star = star_el.text.strip() if star_el else ""  

        title_review_el = rv.select_one("h3.ipc-title__text.ipc-title__text--reduced")
        title_review = title_review_el.text.strip() if title_review_el else "" 

        comment_el = rv.select_one("div.ipc-html-content-inner-div")
        comment = comment_el.text.strip() if comment_el else ""   

        like_el = rv.select_one("span.ipc-voting__label__count.ipc-voting__label__count--up")
        like = like_el.text.strip() if like_el else ""        

        dislike_el = rv.select_one("span.ipc-voting__label__count.ipc-voting__label__count--down")
        dislike = dislike_el.text.strip() if dislike_el else ""  

        date_el = rv.select_one("li.ipc-inline-list__item.review-date")
        date = date_el.text.strip() if date_el else ""       
         
        user_name_el = rv.select_one("a.ipc-link.ipc-link--base")
        user_name = user_name_el.text.strip() if user_name_el else ""        

        reviews.append({
            "review_id" : review_id , 
            "title_review" : title_review ,
            "comment" : comment ,
            "star" : star , 
            "like" : like ,
            "dislike" : dislike ,
            "date" : date ,
            "user_name" : user_name ,
            "movie_id" : movie_id
        })
        num += 1
    return reviews , num
def crawl_data() :
    # ============================================
    # Crawl danh sách phim
    # ============================================
    url = "https://www.imdb.com/chart/top/"
    driver.get(url)
    time.sleep(3)

    soup = BeautifulSoup(driver.page_source, "html.parser")

    # movies = []
    # actors = []
    
    # print('='*90)
    # print("="*33 + "  Starting crawl IMDB  " + "="*33)
    # print('='*90 + "\n")
    num = 1 
    num_rv = 1
    rows = soup.select("ul.ipc-metadata-list li.ipc-metadata-list-summary-item")
    for row in rows[:3]:  
        # print(f"=== Batch {num} ===" + "\n" )
        # num += 1
        # print('='*60)
        # print("=== Starting crawl movies  ===")
        # print('='*60 + "\n")

        title_el = row.select_one("h3.ipc-title__text.ipc-title__text--reduced")
        link_el = row.select_one("a.ipc-title-link-wrapper")
        rating_el = row.select_one("span.ipc-rating-star--rating")
        rating = float(rating_el.text) if rating_el else None
        vote_count_el = row.select_one("span.ipc-rating-star--voteCount")
        spans = [s.text.strip() for s in row.select("div.cli-title-metadata > span.cli-title-metadata-item") ]
        year, duration, items = (spans + [None]*3)[:3]

        if not link_el:
            continue

        movie_url = "https://www.imdb.com" + link_el["href"].split("?")[0]
        movie_id = movie_url.split("/")[-2]

        # print("="*25 + " Crawl movies Successfully " + "="*25 + "\n")

        driver.get(movie_url)
        time.sleep(3)
        actor = BeautifulSoup(driver.page_source, "html.parser")

        # print('='*60)
        # print("=== Starting crawl actors ===")
        # print('='*60+ "\n")
        actor_id = f"A0{num}"
        director, writers, stars, country, language, company, budget, revenue , plot, poster = actors_list(actor)
        # print("="*25 + " Crawl actors Successfully " + "="*25 + "\n")

        review_url = movie_url.rstrip('/') + '/reviews' 
        driver.get(review_url)
        time.sleep(3)

        # print('='*60)
        # print("=== Starting crawl reviews ===")
        # print('='*60 + "\n")

        # try:
        wait = WebDriverWait(driver, 10)
        see_all_button = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "span.ipc-see-more.sc-e2b012eb-0.QEaqv.chained-see-more-button.sc-a8a7adf7-2"
                                                        " button.ipc-btn.ipc-btn--single-padding"
                                                        ".ipc-btn--center-align-content"
                                                        ".ipc-btn--default-height.ipc-btn--core-base"
                                                        ".ipc-btn--theme-base.ipc-btn--button-radius"
                                                        ".ipc-btn--on-accent2"
                                                        ".ipc-text-button"
                                                        ".ipc-see-more__button"))
        )
        driver.execute_script("arguments[0].scrollIntoView(true);", see_all_button)
        time.sleep(1)
        driver.execute_script("arguments[0].click();", see_all_button)
        # print("[INFO] Clicked 'See all' button successfully")
        time.sleep(3)
        # except Exception as e:
            # print("[WARN] 'See all' button not found or not clickable:", str(e))

        wait = WebDriverWait(driver, 10)
        prev_count = 0
        same_count_rounds = 0  
        max_reviews = 300 


        while same_count_rounds < 2:
            try:
                try:
                    load_more = WebDriverWait(driver, 2).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "button.ipc-btn.ipc-btn--on-accent2.ipc-see-more__button"))
                    )
                    driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more)
                    driver.execute_script("arguments[0].click();", load_more)
                    # print("[INFO] Clicked 'Load More'")
                except Exception:
                    same_count_rounds += 1
                    continue

                WebDriverWait(driver, 2).until(
                    lambda d: d.execute_script(
                        "return document.querySelectorAll('section.ipc-page-section article.user-review-item').length;"
                    ) > prev_count
                )

                review_count = driver.execute_script(
                    "return document.querySelectorAll('section.ipc-page-section article.user-review-item').length;"
                )
                # print(f"[INFO] Loaded {review_count} reviews...")

                if review_count == prev_count:
                    same_count_rounds += 1
                else:
                    same_count_rounds = 0
                prev_count = review_count

                if review_count >= max_reviews:
                    # print("[INFO] Enough reviews, stopping early.")
                    break

            except Exception as e:
                # print("Lỗi khi load thêm:", str(e))
                break

        # print("[DONE] Đã load toàn bộ review.")



        review = BeautifulSoup(driver.page_source , "html.parser")
        
        reviews = []
        reviews , num_rv = reviews_list(review , reviews , movie_id , num_rv )

        # print("="*25 + " Crawl reviews Successfully " + "="*25 + "\n")

        movies = {
            "movie_id": movie_id,
            "title": title_el.text.strip(),
            "rating": rating,
            "year": year,
            "vote_count": vote_count_el.text.strip().strip(')').strip('('),
            "runtime": duration,
            "items" : items,
            "country" : country ,
            "language" : language ,
            "company" : company ,
            "budget" : budget ,
            "revenue" : revenue ,
            "plot": plot,
            "poster": poster,
            "url": movie_url
        }

        actors = {
            "actor_id" : actor_id ,
            "director" : director , 
            "writers" : writers , 
            "stars" : stars , 
            "movie_id" : movie_id 
        }
        # emit_batch(movies , actors , reviews)
        emit("movie" , [movies])
        emit("actor" , [actors]) 
        emit("review" , reviews)
        num += 1


        

    # print(f"[INFO] Collected {len(movies)} movies")
    # print(f"[INFO] Collected {len(actors)} actors")
    # print(f"[INFO] Collected {len(reviews)} reviews")

    # movie_df = pd.DataFrame(movies)
    # actor_df = pd.DataFrame(actors)
    # review_df = pd.DataFrame(reviews)

    # print("=== Saved movies to JSON ===")
    # movie_df.to_json("../data/movies.json")
    # print("=== Saved actors to JSON ===")
    # actor_df.to_json("../data/actors.json")
    # print("=== Saved reviews to JSON ===")
    # review_df.to_json("../data/reviews.json")

    # print("="*25 + " All pipeline crawl Successfully " + "="*25)
    
    # return movies, actors, reviews

def emit(tag , records) :
    for r in records :
        r["_type"] = tag 
        print(json.dumps(r , ensure_ascii = False))

def emit_batch(movie_obj, actor_obj, review_list):
    batch = ""

    movie_obj["_type"] = "movie"
    batch += json.dumps(movie_obj, ensure_ascii=False) + "\n"

    actor_obj["_type"] = "actor"
    batch += json.dumps(actor_obj, ensure_ascii=False) + "\n"

    for rv in review_list:
        rv["_type"] = "review"
        batch += json.dumps(rv, ensure_ascii=False) + "\n"

    print(batch)
        
if __name__ == "__main__" :
    crawl_data()

    
