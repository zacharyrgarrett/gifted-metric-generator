DATA_DIR_PATH = "./data"
BUSINESS_DIR_PATH = f"{DATA_DIR_PATH}/business_data"
FIGURES_DIR_PATH = "./figures"
SECRETS_DIR_PATH = "./secrets"

FIREBASE_KEY_PATH = f"{SECRETS_DIR_PATH}/gifted-v1-firebase-adminsdk-2le8w-f76bcaea25.json"
USER_DATA_PATH = f"{DATA_DIR_PATH}/user_data.csv"
FEED_DATA_PATH = f"{DATA_DIR_PATH}/feed_data.csv"
BUSINESS_DATA_PATH = f"{BUSINESS_DIR_PATH}/business_summarized_all_time.csv"
BUSINESS_WEEKLY_PATH = f"{BUSINESS_DIR_PATH}/business_summarized_weekly.csv"
BUSINESS_USER_LOCATIONS_PATH = f"{BUSINESS_DIR_PATH}/business_user_locations.csv"
BUSINESS_POSTS_USER_CATEGORY_PATH = f"{BUSINESS_DIR_PATH}/business_influencer_categories.csv"

class FigurePaths:
    FEED_BY_BUSINESS_CATEGORY = f"{FIGURES_DIR_PATH}/feed_by_business_category.html"
    FIRST_POST_BY_WEEK = f"{FIGURES_DIR_PATH}/first_post_by_week.html"
    WORLD_MAP_PATH = f"{FIGURES_DIR_PATH}/world_users.html"
    USA_MAP_PATH = f"{FIGURES_DIR_PATH}/usa_users.html"
    USER_CATEGORIES = f"{FIGURES_DIR_PATH}/user_categories.html"
    USER_SOCIALS = f"{FIGURES_DIR_PATH}/user_socials.html"


COMMON_BUSINESS_NAMES = [
    "Alixandra Blue",
    "Beginning Boutique",
    "Boutine LA",
    "Bright Swimwear",
    "Colorful Natalie",
    "Edikted",
    "PrettyLittleThing",
    "Princess Polly",
]