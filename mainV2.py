"""
Version N2
This script is better than the 1st one. returns 25.3K lines from the merge results.
This script made by Psycoments06 feat ChatGpt3 that's why it has problems. It gives great results, but
it kills your system :). It uses more than 10GB or Memory, so it's not suitable but kept for reference.

See notebooks/unsplash.ipynb for results
"""
import pandas as pd

# Read the three CSV files
photos_df = pd.read_csv("./data/photos.tsv000", sep='\t', header=0)
tags_df = pd.read_csv("./data/keywords.tsv000", sep='\t', header=0)
category_df = pd.read_csv("./data/collections.tsv000", sep='\t', header=0)

# Merge the dataframes
merged_df = photos_df.merge(tags_df, on="photo_id", how="left")
merged_df = merged_df.merge(category_df, on="photo_id", how="left")

# Group and aggregate the keywords and collection titles
merged_df["keywords"] = merged_df.groupby("photo_id")["keyword"].transform(lambda x: ",".join(x))
merged_df["collections"] = merged_df.groupby("photo_id")["collection_title"].transform(lambda x: ",".join(x))

# Drop duplicates and unnecessary columns
merged_df = merged_df.drop_duplicates(subset=["photo_id"])

# Filter out rows without a corresponding photo_id
merged_df = merged_df[~merged_df["photo_id"].isnull()]

# Reset the index
merged_df.reset_index(drop=True, inplace=True)

# Rename columns
merged_df = merged_df.rename(columns={
    'photo_id': 'id',
    'photo_image_url': 'image_url',
    'photo_description': 'title',
    'ai_description': 'description',
    'keyword': 'tags',
    'collection_title': 'category'
    })

# Save the merged dataframe to a CSV file
merged_df.to_csv('./data/export/photos_with_tags.tsv', sep='\t', index=False)

