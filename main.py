"""
Version N1
This is the 1st try to merge and get valid data from the photos, keywords and collections datasets
Works but not perfect
"""
import pandas as pd

photos_df = pd.read_csv("./data/photos.tsv000", sep='\t', header=0)
tags_df = pd.read_csv("./data/keywords.tsv000", sep='\t', header=0)
category_df = pd.read_csv("./data/collections.tsv000", sep='\t', header=0)


# patterns = r'[@#&$%+-_~\'"`\\/*0-9@$-?]'
# photos_df = photos_df.drop(photos_df[photos_df['photo_image_url'].str.len() == 0].index)
# tags_df = tags_df.drop(tags_df[(tags_df['keyword'].str.len() == 0) & (tags_df['keyword'].str.len() > 20) & (tags_df['keyword'].str.contains(patterns))].index)
# category_df = category_df .drop(category_df[(category_df['collection_title'].str.len() == 0) & (category_df['collection_title'].str.len() > 20) & (category_df['collection_title'].str.contains(patterns))].index)

tags_df = tags_df.dropna()
tags_df = tags_df.groupby('photo_id')['keyword'].apply(','.join).reset_index()

category_df['collection_title'] = category_df['collection_title'].astype(str)
category_df = category_df.dropna()
category_df = category_df.groupby('photo_id')['collection_title'].apply(','.join).reset_index()
new_df = photos_df.merge(tags_df, on='photo_id', how='left').merge(category_df, on='photo_id', how='left')
new_df = new_df[['photo_id', 'photo_image_url', 'photo_description', 'ai_description', 'keyword', 'collection_title']]


new_df = new_df.rename(columns={
    'photo_id': 'id',
    'photo_image_url': 'image_url',
    'photo_description': 'title',
    'ai_description': 'description',
    'keyword': 'tags',
    'collection_title': 'category'
    })

new_df.to_csv('./data/export/photos_with_tags.tsv', sep='\t', index=False)
