# from openai import OpenAI
# import base64
# import json
# import requests
# from shared import API_KEY

# client = OpenAI(api_key=API_KEY)

# product = {
#     "part_title": "Lenovo Laptop Battery",
#     "part_number": "5B10W67285",
#     "product_image": "https://cdn.shopify.com/s/files/1/0700/0293/3937/files/LNVO5B10W67285_R01_C02.jpg?v=1762644481",
#     "rating": 4,
#     "footer_points": [
#         "The Lenovo Laptop Battery is a vital component for your device.",
#         "Designed for Best Fit.",
#         "OEM Quality Assurance.",
#         "Durable Construction.",
#         "Easy DIY Installation."
#     ]
# }

# # Download product image
# img = requests.get(product["product_image"]).content
# with open("product.jpg", "wb") as f:
#     f.write(img)

# # Dynamic prompt enforcing layout, star counts, and text boundaries
# prompt = f"""
# Recreate the first image (sample.png) while strictly preserving its exact layout, dimensions, font styles, and card sizes. Do not change the grid or scale down any containers.

# Replace only the following dynamic content using the second uploaded image:

# 1. Header text:
#    - Title: Change to "{product['part_title']}"
#    - Part Number sub-header pill: Change to "{product['part_number']}"

# 2. Central Image:
#    - Replace the product image inside the circle with the second uploaded image. Ensure it fits cleanly inside the circular border without overflowing.

# 3. Customer Reviews Section:
#    - Rating: Exactly {product['rating']} out of 5 stars. 
#    - Fill exactly {product['rating']} stars with gold/yellow coloring. The remaining star(s) must be an empty outline.
#    - Text below stars: Update to read exactly "{float(product['rating'])} OUT OF 5 STARS".

# 4. Safety Tip Section:
#    - Provide a concise, relevant safety tip for this specific part type. Adjust the sentence length so it fits neatly into the card without running out of bounds or forcing text wrapping issues.

# 5. Footer Points & Icons:
#    - Map the following strings sequentially to the 4 footer slots: {json.dumps(product['footer_points'][:4])}
#    - For each slot, generate a short 2-3 word bold title that summarizes the point.
#    - Ensure all text is typed out perfectly with high-quality, crisp characters. Avoid gibberish or overlapping text.
# """

# result = client.images.edit(
#     model="gpt-image-1",
#     image=[
#         open("sample.png", "rb"),
#         open("product.jpg", "rb"),
#     ],
#     prompt=prompt,
#     size="1024x1024",
# )

# image_bytes = base64.b64decode(result.data[0].b64_json)

# with open("output.png", "wb") as f:
#     f.write(image_bytes)

# print("Done")


import base64
import json
import requests
from openai import OpenAI
from shared import API_KEY

client = OpenAI(api_key=API_KEY)

# Dynamic product data dictionary
product = {
    "part_title": "GE Refrigerator Top Hinge & Pin (Right)",
    "part_number": "WR13X28532",
    "product_image": "https://cdn.shopify.com/s/files/1/0700/0293/3937/files/GEWR13X28532.jpg?v=1762638428",
    "rating": 3,
    "footer_points": [
        "The GE Refrigerator Top Hinge & Pin (Right) is a vital component for your device.",
        "Backed by the manufacturer's guarantee, this hinge ensures long-lasting performance and reliability.",
        "Built to withstand harsh environments, this hinge is designed for durability and longevity.",
        "Simple installation process, no tools required, making it easy for DIY enthusiasts.",
    ],
}

# Download the dynamic product image
img = requests.get(product["product_image"]).content
with open("product.jpg", "wb") as f:
    f.write(img)

# Generate explicit structured titles to pair with descriptions for the grid layout
footer_titles = [
    "VITAL COMPONENT",
    "PRECISE COMPATIBILITY",
    "OEM QUALITY",
    "DURABLE DESIGN",
]

# Map your list data into specific, short-length roles to avoid text-wrapping issues
footer_rows = []
for i in range(4):
    footer_rows.append(
        {"title": footer_titles[i], "desc": product["footer_points"][i]}
    )

# Dynamic prompt enforcing layout, star counts, and explicit footer grid alignment
prompt = f"""
Recreate the infographic from the first image (sample.png), maintaining the exact size, position, and font styles of all cards, the 'Easy'/'Yes' statuses, and the '15-30' time estimate.

Apply the following dynamic updates using the second uploaded image:

1.  **Header:** Change title to "{product['part_title']}" and the sub-header pill text to "{product['part_number']}".
2.  **Image:** Replace the central image with the laptop battery (the second input) and ensure it fits perfectly within the circular frame.
3.  **Customer Reviews:** Preserve the card size. Visually set the rating to precisely {product['rating']} full gold/yellow stars and the remainder to clear outlines. Update the text below to read exactly "{float(product['rating'])} OUT OF 5 STARS". *Critical: All text must be spelled correctly.*
4.  **Safety Tip:** Generate a concise safety tip appropriate for *laptop battery replacement* that fits neatly into the card (e.g., disconnecting power, avoiding puncture).
5.  **Footer Section (Alignment and Correction):** The entire blue footer section must be restructured into a strict, perfectly aligned four-column, two-row grid.
    * *Alignment Rule:* Ensure the titles (top row), descriptions (bottom row), and icons across all columns are shared on common vertical and horizontal baselines, creating a perfectly squared, tidy arrangement. No part of any column should sag or appear misaligned.
    * *Correction:* All text must be typed perfectly and spelled correctly (e.g., "Lenovo" and "Battery").
    * *Mapping:*
        - **Column 1:** Maintain the Gear/Component icon. Title: "{footer_rows[0]['title']}", Description: "{footer_rows[0]['desc']}".
        - **Column 2:** Maintain the Thumbs-up/Best Fit icon. Title: "{footer_rows[1]['title']}", Description: "{footer_rows[1]['desc']}".
        - **Column 3:** Maintain the Shield/Quality icon. Title: "{footer_rows[2]['title']}", Description: "{footer_rows[2]['desc']}".
        - **Column 4:** Maintain the Construction/Puzzle/Fit icon. Title: "{footer_rows[3]['title']}", Description: "{footer_rows[3]['desc']}".
"""

# Call the image edit API
result = client.images.edit(
    model="gpt-image-1",
    image=[
        open("sample.png", "rb"),
        open("product.jpg", "rb"),
    ],
    prompt=prompt,
    size="1024x1024",
)

# Decode and save the output image
image_bytes = base64.b64decode(result.data[0].b64_json)

with open("output.png", "wb") as f:
    f.write(image_bytes)

print("Done")