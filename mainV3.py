"""
Version N3
This is we can the perfect one. no, thanks to ChatGpt this time.
Tuned for better performance and with removing all dirty data (non english/latin columns, alphanumeric, ...)
See notebooks/unsplash_v2.ipnyb for results
"""
import pandas as pd
import numpy as np
import unicodedata
import re
import os
import time


# little lazy hack :)
def _print(o: str):
    o += " ........."
    print(o)


def is_valid(val: str):
    pattern = r'[^a-zA-Z\s]'
    pat = re.compile(pattern)
    if pat.search(val):
        return True
    return False


def is_too_long(val: str, max: int = 20):
    return len(val) > max


def is_ascii(val: str):
    for c in val:
        try:
            n = unicodedata.name(c).startswith("LATIN")
            if n:
                return True
        except ValueError:
            return False
    return False


def is_id_valid(id: str):
    return len(id) == 11


start_time = time.time()
_print("Loading collections.tsv000")
category_df = pd.read_csv('./data/collections.tsv000', sep='\t', header=0)
_print("Converting 'collection_title' to string")
category_df['collection_title'] = category_df['collection_title'].astype(str)
_print("Removing long collection titles")
category_df = category_df[~category_df['collection_title'].apply(is_too_long, args=(20,))]
_print('Removing  non-english/latin columns')
category_df = category_df[~category_df['collection_title'].apply(is_valid)]
_print("'collection_titles'.lower()")
category_df['collection_title'] = category_df['collection_title'].str.lower()
category_df = category_df[category_df['collection_title'].apply(is_ascii)]
_print("Getting most used/referenced category titles")
category_df = category_df.groupby(['photo_id', 'collection_title']).size().reset_index(name='count')
first_most_used_titles = category_df.groupby('photo_id').apply(
    lambda x: x.sort_values('count', ascending=False).head(1)).reset_index(drop=True)
category_df = category_df.merge(first_most_used_titles, on='photo_id', suffixes=('', '_first_most_used'))
del first_most_used_titles
category_df = category_df.drop(columns=['collection_title_first_most_used'])
category_df = category_df[['collection_title', 'photo_id']]

_print("Loading  keywords.tsv000")
tag_df = pd.read_csv('./data/keywords.tsv000', sep='\t', header=0)
_print("Converting 'keyword' column to string")
tag_df['keyword'] = tag_df['keyword'].astype(str)
_print("Removing long keywords")
tag_df = tag_df[~tag_df['keyword'].apply(is_too_long, args=(10,))]
_print("Removing non-english/latin columns")
tag_df = tag_df[~tag_df['keyword'].apply(is_valid)]
tag_df['keyword'] = tag_df['keyword'].str.lower()
tag_df = tag_df[tag_df['keyword'].apply(is_ascii)]
_print("Getting most used keywords")
tag_df = tag_df.groupby(['photo_id', 'keyword']).size().reset_index(name='count')
first_most_used_keywords = tag_df.groupby('photo_id').apply(
    lambda x: x.sort_values('count', ascending=False).head(10)).reset_index(drop=True)
tag_df = tag_df.merge(first_most_used_keywords, on='photo_id', suffixes=('', '_first_most_used'))
del first_most_used_keywords
tag_df = tag_df.drop(columns=['keyword_first_most_used'])
tag_df = tag_df.drop_duplicates(subset=['keyword'])
tag_df = tag_df.groupby("photo_id")['keyword'].apply(','.join).reset_index()
tag_df = tag_df[['keyword', 'photo_id']]

_print("Loading  photos.tsv000")
photo_df = pd.read_csv('./data/photos.tsv000', sep='\t', header=0)
photo_df = photo_df[['photo_id', 'photo_image_url', 'photo_description', 'ai_description']]

_print("Merging with tags.tsv")
new_df = photo_df.merge(tag_df, on="photo_id", how="left")
_print("Merging with categories.tsv")
new_df = new_df.merge(category_df, on="photo_id", how="left")
_print("Remove invalid photo_id's")
new_df = new_df[new_df['photo_id'].apply(is_id_valid)]
_print("Remove duplicate photo_id's")
new_df = new_df.drop_duplicates(subset=['photo_id'])
new_df.reset_index(drop=True, inplace=True)

_print("Rename  columns")
new_df = new_df.rename(columns={
    'photo_id': 'id',
    'photo_image_url': 'image_url',
    'photo_description': 'title',
    'ai_description': 'description',
    'keyword': 'tags',
    'collection_title': 'category'
})

_print("Saving new file to " + os.getcwd() + "/data/export/photos_data.tsv")
chunks = np.array_split(new_df, 25)
for i, chunk in enumerate(chunks):
    pd_chunk = pd.DataFrame(chunk)
    pd_chunk.to_csv(f'./data/export/photos_data_{i}.tsv', sep='\t', index=False)

finished = time.time() - start_time
_print("Execution time: " + str(round(finished)) + " seconds")
