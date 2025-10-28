import pandas as pd

region_df = pd.read_csv("Countries.csv")
mineral_df = pd.read_csv("mineral.csv")

region_df["Country"] = region_df["Country"].str.strip()
mineral_df["reporterDesc"] = mineral_df["reporterDesc"].str.strip()

# Right join to preserve all Super-region countries
merged_df = pd.merge(
    mineral_df,
    region_df,
    how="right",
    left_on="reporterDesc",
    right_on="Country"
)

merged_df.to_csv("Countries.csv", index=False)


from PIL import Image

filenames = [
    "ida_vs_cpia_avg_2019.png",
    "ida_vs_cpia_avg_2020.png",
    "ida_vs_cpia_avg_2021.png",
    "ida_vs_cpia_avg_2022.png",
    "ida_vs_cpia_avg_2023.png"
]

images = [Image.open(fname) for fname in filenames]
img_width, img_height = images[0].size

cols = 3
rows = 2
grid_width = cols * img_width
grid_height = rows * img_height

combined_img = Image.new("RGB", (grid_width, grid_height), color="white")

# Paste each image into the grid
for idx, img in enumerate(images):
    x = (idx % cols) * img_width
    y = (idx // cols) * img_height
    combined_img.paste(img, (x, y))

combined_img.save("ida_vs_cpia_avg_combined.png")

