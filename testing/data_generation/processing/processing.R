library('dplyr')
library('readr')
library('tidyr')

# define size of test sample
test_sample_size = 25000L

# define input directory paths
input_dir.airbnb ='../input/airbnb/'
input_dir.fueleconomy ='../input/fueleconomy/'
input_dir.coil2000 = '../input/coil2000/'
input_dir.nba = '../input/nba/'

input_dir.store_sales = '../input/store_sales/'

# define output directory paths
output_dir.airbnb ='../output/airbnb/'
output_dir.fueleconomy ='../output/fueleconomy/'
output_dir.coil2000 = '../output/coil2000/'
output_dir.nba = '../output/nba/'

output_dir.store_sales = '../output/store_sales/'

# define auxiliary output directories
output_dir.airbnb.aux <- paste(output_dir.airbnb, 'cities/', sep='')

# define list of all output directories
output_dirs = 
  c(output_dir.airbnb, output_dir.fueleconomy,
    output_dir.coil2000, output_dir.nba,
    output_dir.airbnb.aux)

# create output directories if they do not exist
lapply(output_dirs, dir.create, showWarnings = FALSE)

# column name definitions
# (only used for data sets that have no header)
column_names.coil2000 <- c(
  'ID', 'MOSTYPE', 'MAANTHUI', 'MGEMOMV', 'MGEMLEEF', 'MOSHOOFD', 'MGODRK',
  'MGODPR', 'MGODOV', 'MGODGE', 'MRELGE', 'MRELSA', 'MRELOV', 'MFALLEEN',
  'MFGEKIND', 'MFWEKIND', 'MOPLHOOG', 'MOPLMIDD', 'MOPLLAAG', 'MBERHOOG',
  'MBERZELF', 'MBERBOER', 'MBERMIDD', 'MBERARBG', 'MBERARBO', 'MSKA',
  'MSKB1', 'MSKB2', 'MSKC', 'MSKD', 'MHHUUR', 'MHKOOP', 'MAUT1', 'MAUT2',
  'MAUT0', 'MZFONDS', 'MZPART', 'MINKM30', 'MINK3045', 'MINK4575', 'MINK7512',
  'MINK123M', 'MINKGEM', 'MKOOPKLA', 'PWAPART', 'PWABEDR', 'PWALAND',
  'PPERSAUT', 'PBESAUT', 'PMOTSCO', 'PVRAAUT', 'PAANHANG', 'PTRACTOR',
  'PWERKT', 'PBROM', 'PLEVEN', 'PPERSONG', 'PGEZONG', 'PWAOREG', 'PBRAND',
  'PZEILPL', 'PPLEZIER', 'PFIETS', 'PINBOED', 'PBYSTAND', 'AWAPART',
  'AWABEDR', 'AWALAND', 'APERSAUT', 'ABESAUT', 'AMOTSCO', 'AVRAAUT', 'AAANHANG',
  'ATRACTOR', 'AWERKT', 'ABROM', 'ALEVEN', 'APERSONG', 'AGEZONG', 'AWAOREG',
  'ABRAND', 'AZEILPL', 'APLEZIER', 'AFIETS', 'AINBOED', 'ABYSTAND', 'CARAVAN'
)

# full column definitions
# (used when the schema cannot be automatically detected consistently)
# (especially useful if multiple .csv files are read which may yield different detected schemata)
col_def.airbnb <- cols(
  id = col_integer(),
  listing_url = col_character(),
  scrape_id = col_integer(),
  last_scraped = col_date(format = ''),
  name = col_character(),
  description = col_character(),
  neighborhood_overview = col_character(),
  picture_url = col_character(),
  host_id = col_integer(),
  host_url = col_character(),
  host_name = col_character(),
  host_since = col_date(format = ''),
  host_location = col_character(),
  host_about = col_character(),
  host_response_time = col_character(),
  host_response_rate = col_character(),
  host_acceptance_rate = col_character(),
  host_is_superhost = col_logical(),
  host_thumbnail_url = col_character(),
  host_picture_url = col_character(),
  host_neighbourhood = col_character(),
  host_listings_count = col_double(),
  host_total_listings_count = col_double(),
  host_verifications = col_character(),
  host_has_profile_pic = col_logical(),
  host_identity_verified = col_logical(),
  neighbourhood = col_character(),
  neighbourhood_cleansed = col_character(),
  neighbourhood_group_cleansed = col_logical(),
  latitude = col_double(),
  longitude = col_double(),
  property_type = col_character(),
  room_type = col_character(),
  accommodates = col_integer(),
  bathrooms = col_integer(),
  bathrooms_text = col_character(),
  bedrooms = col_integer(),
  beds = col_integer(),
  amenities = col_character(),
  price = col_character(),
  minimum_nights = col_double(),
  maximum_nights = col_double(),
  minimum_minimum_nights = col_double(),
  maximum_minimum_nights = col_double(),
  minimum_maximum_nights = col_double(),
  maximum_maximum_nights = col_double(),
  minimum_nights_avg_ntm = col_double(),
  maximum_nights_avg_ntm = col_double(),
  calendar_updated = col_logical(),
  has_availability = col_logical(),
  availability_30 = col_double(),
  availability_60 = col_double(),
  availability_90 = col_double(),
  availability_365 = col_double(),
  calendar_last_scraped = col_date(format = ''),
  number_of_reviews = col_double(),
  number_of_reviews_ltm = col_double(),
  number_of_reviews_l30d = col_double(),
  first_review = col_date(format = ''),
  last_review = col_date(format = ''),
  review_scores_rating = col_double(),
  review_scores_accuracy = col_double(),
  review_scores_cleanliness = col_double(),
  review_scores_checkin = col_double(),
  review_scores_communication = col_double(),
  review_scores_location = col_double(),
  review_scores_value = col_double(),
  license = col_logical(),
  instant_bookable = col_logical(),
  calculated_host_listings_count = col_double(),
  calculated_host_listings_count_entire_homes = col_double(),
  calculated_host_listings_count_private_rooms = col_double(),
  calculated_host_listings_count_shared_rooms = col_double(),
  reviews_per_month = col_double()
)

# process AirBnB dataset
files.airbnb <- list.files(path=input_dir.airbnb, pattern='*.csv')
lapply(files.airbnb, function(x) {
  df.airbnb <- read_csv(paste(input_dir.airbnb, x, sep=''), col_types = col_def.airbnb)
  
  df.airbnb.selection <- df.airbnb %>% select(
    id,
    accommodates,                # maximum capacity
    bedrooms,                    # number of bedrooms
    beds,                        # number of beds
    price,                       # price
    number_of_reviews,           # number of reviews
    review_scores_rating,        # review score: rating
    review_scores_accuracy,      # review score: accuracy
    review_scores_cleanliness,   # review score: cleanliness
    review_scores_checkin,       # review score: check-in
    review_scores_communication, # review score: communication
    review_scores_location,      # review score: location
    review_scores_value          # review score: value
  ) %>% mutate(price = parse_number(price)) %>%
    arrange(id)
  
  write_csv(df.airbnb.selection, paste(output_dir.airbnb.aux, x, sep=''), na='')
})

df.airbnb <- list.files(path=output_dir.airbnb.aux, full.names = TRUE) %>% 
  lapply(read_csv, col_types = col_def.airbnb) %>% bind_rows %>% arrange(id) %>% distinct()
df.airbnb.complete <- na.omit(df.airbnb)

df.airbnb.test <- df.airbnb %>%
  slice_head(n = test_sample_size)
df.airbnb.complete.test <- df.airbnb.complete %>%
  slice_head(n = test_sample_size)

write_csv(df.airbnb, paste(output_dir.airbnb, 'airbnb_incomplete.csv', sep=''), na='')
write_csv(df.airbnb.complete, paste(output_dir.airbnb, 'airbnb.csv', sep=''), na='')

write_csv(df.airbnb.test, paste(output_dir.airbnb, 'airbnb_incomplete_test.csv', sep=''), na='')
write_csv(df.airbnb.complete.test, paste(output_dir.airbnb, 'airbnb_test.csv', sep=''), na='')

# process fueleconomy dataset
df.fueleconomy.full <- read_csv(paste(input_dir.fueleconomy, 'vehicles.csv', sep=''))
df.fueleconomy <- df.fueleconomy.full %>%
  select(make, model, cylinders, displ, combinedCD, fuelCost08, comb08,
         barrels08, city08, cityUF, highway08) %>%
  distinct()
df.fueleconomy.complete = na.omit(df.fueleconomy)

df.fueleconomy.test <- df.fueleconomy %>%
  slice_head(n = test_sample_size)
df.fueleconomy.complete.test <- df.fueleconomy.complete %>%
  slice_head(n = test_sample_size)

write_csv(df.fueleconomy.complete, paste(output_dir.fueleconomy, 'fueleconomy.csv', sep=''), na='')
write_csv(df.fueleconomy, paste(output_dir.fueleconomy, 'fueleconomy_incomplete.csv', sep=''), na='')

write_csv(df.fueleconomy.complete.test, paste(output_dir.fueleconomy, 'fueleconomy_test.csv', sep=''), na='')
write_csv(df.fueleconomy.test, paste(output_dir.fueleconomy, 'fueleconomy_incomplete_test.csv', sep=''), na='')

# process coil2000 dataset
df.coil2000 <- read_tsv(
    paste(input_dir.coil2000, 'ticdata2000.txt', sep=''),
    col_names=FALSE) %>%
  mutate(id = row_number()) %>%
  relocate(id) %>%
  distinct()

names(df.coil2000) <- column_names.coil2000

df.coil2000.complete <- na.omit(df.coil2000)

df.coil2000.test <- df.coil2000 %>%
  slice_head(n = test_sample_size)
df.coil2000.complete.test <- na.omit(df.coil2000) %>%
  slice_head(n = test_sample_size)

write_csv(df.coil2000.complete, paste(output_dir.coil2000, 'coil2000.csv', sep=''), na='')
write_csv(df.coil2000, paste(output_dir.coil2000, 'coil2000_incomplete.csv', sep=''), na='')

write_csv(df.coil2000.complete.test, paste(output_dir.coil2000, 'coil2000_test.csv', sep=''), na='')
write_csv(df.coil2000.test, paste(output_dir.coil2000, 'coil2000_incomplete_test.csv', sep=''), na='')

# process NBA dataset
df.nba <- read_csv(paste(input_dir.nba, 'stats.csv', sep=''))  %>%
  distinct()
df.nba.complete <- na.omit(df.nba)

df.nba.test <- df.nba %>%
  slice_head(n = test_sample_size)
df.nba.complete.test <- df.nba.complete %>%
  slice_head(n = test_sample_size)

write_csv(df.nba.complete, paste(output_dir.nba, 'nba.csv', sep=''), na='')
write_csv(df.nba, paste(output_dir.nba, 'nba_incomplete.csv', sep=''), na='')

write_csv(df.nba.complete.test, paste(output_dir.nba, 'nba_test.csv', sep=''), na='')
write_csv(df.nba.test, paste(output_dir.nba, 'nba_incomplete_test.csv', sep=''), na='')

df.store_sales.incomplete <- read.csv(paste(input_dir.store_sales, 'store_sales.csv', sep=''))
df.store_sales.complete <-df.store_sales.incomplete %>%
  drop_na(ss_quantity, ss_wholesale_cost, ss_list_price,
          ss_sales_price, ss_ext_discount_amt, ss_ext_sales_price)

write_csv(df.store_sales.incomplete %>% slice_head(n = 1e+06), paste(output_dir.store_sales, 'store_sales_incomplete_1M.csv', sep=''), na='')
write_csv(df.store_sales.incomplete %>% slice_head(n = 2e+06), paste(output_dir.store_sales, 'store_sales_incomplete_2M.csv', sep=''), na='')
write_csv(df.store_sales.incomplete %>% slice_head(n = 5e+06), paste(output_dir.store_sales, 'store_sales_incomplete_5M.csv', sep=''), na='')
write_csv(df.store_sales.incomplete %>% slice_head(n = 10e+06), paste(output_dir.store_sales, 'store_sales_incomplete_10M.csv', sep=''), na='')

write_csv(df.store_sales.complete %>% slice_head(n = 1e+06), paste(output_dir.store_sales, 'store_sales_1M.csv', sep=''), na='')
write_csv(df.store_sales.complete %>% slice_head(n = 2e+06), paste(output_dir.store_sales, 'store_sales_2M.csv', sep=''), na='')
write_csv(df.store_sales.complete %>% slice_head(n = 5e+06), paste(output_dir.store_sales, 'store_sales_5M.csv', sep=''), na='')
write_csv(df.store_sales.complete %>% slice_head(n = 10e+06), paste(output_dir.store_sales, 'store_sales_10M.csv', sep=''), na='')
