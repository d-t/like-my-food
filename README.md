# Like my Food

## Scraper

### Example
```python
import os

from scraper import Scraper


# Initialize scraper
path_media = os.path.join('output', 'db', 'df_media.csv')
path_users = os.path.join('output', 'db', 'df_users.csv')
ig_scraper = Scraper(path_df_media=path_media, 
                     path_df_users=path_users, 
                     idx_first=10)

# Use scraper
ig_scraper.insert_in_db(n_iterations=10)

# Write results on file
ig_scraper.write_db_on_file()
```