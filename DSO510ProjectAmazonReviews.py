import pandas as pd
import statistics
import glob

#import certain columns of a .tsv file and set the datatypes of the columns
def importClean(datasetFile):
    df = pd.read_csv(datasetFile, sep='\t', usecols = [6,7,8,9,11,14])
    df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
    df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
    df['helpful_votes'] = pd.to_numeric(df['helpful_votes'], errors='coerce')
    df['total_votes'] = pd.to_numeric(df['total_votes'], errors='coerce')
    
    return df

#get the average star rating for a selection of months as well as the avg number of reviews for each month
def avgRating(dataset, months):
    selectedMonths = []
    for month in months:
        monthratings = dataset[dataset['review_date'].dt.month == month]
        ratingValues = list(monthratings['star_rating'])
        selectedMonths.extend(ratingValues)
    print('for months',months)
    print('# of items: ', len(selectedMonths))
    print('avg # of reviews per month', len(selectedMonths)/len(months))
    return statistics.mean(selectedMonths)


#create an empty dataframe to be added to later
reviews = pd.DataFrame(columns = ['star_rating','review_date'])

#modify this to set the path to where the datasets are
path = ''

#use this if you want to look at only one .csv file (Gift_Card is the smallest dataset)
#allFiles = glob.glob(path + 'amazon_reviews_us_Gift_Card_v1_00.tsv')

allFiles = glob.glob(path + 'amazon_reviews_us_*.tsv')
for i in allFiles:
    print('adding ',i)
    reviews = reviews.append(importClean(i))

print('avg rating:', avgRating(reviews, [11,12,1]))
print('--------------------------')
print('avg rating:', avgRating(reviews,[2,3,4,5,6,7,8,9,10]))

#set this to the path where you want the subset file to go
reviews.to_csv('')
