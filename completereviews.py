import pandas as pd
import statistics
import glob

def importClean(datasetFile):
    df = pd.read_csv(datasetFile, low_memory=True,sep='\t',usecols=[0,1,2,3,4,5,6,7,8,9,10,11,12,13,14])
    #df = df.sample(6000)
    df['review_date'] = pd.to_datetime(df['review_date'], errors='coerce')
    df['star_rating'] = pd.to_numeric(df['star_rating'], errors='coerce')
    df['helpful_votes'] = pd.to_numeric(df['helpful_votes'], errors='coerce')
    df['total_votes'] = pd.to_numeric(df['total_votes'], errors='coerce')
    #print(df)
    
    return df

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



reviews = pd.DataFrame(columns = ['marketplace','customer_id','review_id','product_id','product_parent','product_title','product_category','star_rating','helpful_votes','total_votes','vine','verified_purchase','review_headline','review_body','review_date'])

#modify this to set the path to where the datasets are
path = '/home/ec2-user/reviews/USreviews/'

#use this if you want to look at only one .csv file
#allFiles = glob.glob(path + 'amazon_reviews_us_Gift_Card_v1_00.tsv')

allFiles = glob.glob(path + 'amazon_reviews_us_*.tsv')
for i in allFiles:
    print('adding ',i)
    reviews = reviews.append(importClean(i))

#print('avg rating:', avgRating(reviews, [11,12,1]))
#print('--------------------------')
#print('avg rating:', avgRating(reviews,[2,3,4,5,6,7,8,9,10]))
print('dataframe constructed')
#set this to the path where you want the subset file to go
reviews.to_csv('/home/ec2-user/COMPLETEREVIEWS.csv')
