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


# import base64
# import json
# import requests
# from openai import OpenAI
# from shared import API_KEY

# client = OpenAI(api_key=API_KEY)

# # Dynamic product data dictionary
# product = {
#     "part_title": "GE Refrigerator Top Hinge & Pin (Right)",
#     "part_number": "WR13X28532",
#     "product_image": "https://cdn.shopify.com/s/files/1/0700/0293/3937/files/GEWR13X28532.jpg?v=1762638428",
#     "rating": 3,
#     "footer_points": [
#         "The GE Refrigerator Top Hinge & Pin (Right) is a vital component for your device.",
#         "Backed by the manufacturer's guarantee, this hinge ensures long-lasting performance and reliability.",
#         "Built to withstand harsh environments, this hinge is designed for durability and longevity.",
#         "Simple installation process, no tools required, making it easy for DIY enthusiasts.",
#     ],
# }

# # Download the dynamic product image
# img = requests.get(product["product_image"]).content
# with open("product.jpg", "wb") as f:
#     f.write(img)

# # Generate explicit structured titles to pair with descriptions for the grid layout
# footer_titles = [
#     "VITAL COMPONENT",
#     "PRECISE COMPATIBILITY",
#     "OEM QUALITY",
#     "DURABLE DESIGN",
# ]

# # Map your list data into specific, short-length roles to avoid text-wrapping issues
# footer_rows = []
# for i in range(4):
#     footer_rows.append(
#         {"title": footer_titles[i], "desc": product["footer_points"][i]}
#     )

# # Dynamic prompt enforcing layout, star counts, and explicit footer grid alignment
# prompt = f"""
# Recreate the infographic from the first image (sample.png), maintaining the exact size, position, and font styles of all cards, the 'Easy'/'Yes' statuses, and the '15-30' time estimate.

# Apply the following dynamic updates using the second uploaded image:

# 1.  **Header:** Change title to "{product['part_title']}" and the sub-header pill text to "{product['part_number']}".
# 2.  **Image:** Replace the central image with the laptop battery (the second input) and ensure it fits perfectly within the circular frame.
# 3.  **Customer Reviews:** Preserve the card size. Visually set the rating to precisely {product['rating']} full gold/yellow stars and the remainder to clear outlines. Update the text below to read exactly "{float(product['rating'])} OUT OF 5 STARS". *Critical: All text must be spelled correctly.*
# 4.  **Safety Tip:** Generate a concise safety tip appropriate for *laptop battery replacement* that fits neatly into the card (e.g., disconnecting power, avoiding puncture).
# 5.  **Footer Section (Alignment and Correction):** The entire blue footer section must be restructured into a strict, perfectly aligned four-column, two-row grid.
#     * *Alignment Rule:* Ensure the titles (top row), descriptions (bottom row), and icons across all columns are shared on common vertical and horizontal baselines, creating a perfectly squared, tidy arrangement. No part of any column should sag or appear misaligned.
#     * *Correction:* All text must be typed perfectly and spelled correctly (e.g., "Lenovo" and "Battery").
#     * *Mapping:*
#         - **Column 1:** Maintain the Gear/Component icon. Title: "{footer_rows[0]['title']}", Description: "{footer_rows[0]['desc']}".
#         - **Column 2:** Maintain the Thumbs-up/Best Fit icon. Title: "{footer_rows[1]['title']}", Description: "{footer_rows[1]['desc']}".
#         - **Column 3:** Maintain the Shield/Quality icon. Title: "{footer_rows[2]['title']}", Description: "{footer_rows[2]['desc']}".
#         - **Column 4:** Maintain the Construction/Puzzle/Fit icon. Title: "{footer_rows[3]['title']}", Description: "{footer_rows[3]['desc']}".
# """

# # Call the image edit API
# result = client.images.edit(
#     model="gpt-image-1",
#     image=[
#         open("sample.png", "rb"),
#         open("product.jpg", "rb"),
#     ],
#     prompt=prompt,
#     size="1024x1024",
# )

# # Decode and save the output image
# image_bytes = base64.b64decode(result.data[0].b64_json)

# with open("output.png", "wb") as f:
#     f.write(image_bytes)

# print("Done")


# import base64
# import json
# import requests
# from openai import OpenAI
# from shared import API_KEY

# client = OpenAI(api_key=API_KEY)

# # Dynamic product data dictionary
# product = {
#     "part_title": "GE Refrigerator Top Hinge & Pin (Right)",
#     "part_number": "WR13X28532",
#     "product_image": "https://cdn.shopify.com/s/files/1/0700/0293/3937/files/GEWR13X28532.jpg?v=1762638428",
#     "rating": 3,
#     "footer_points": [
#         "The GE Refrigerator Top Hinge & Pin (Right) is a vital component for your device.",
#         "Backed by the manufacturer's guarantee, this hinge ensures long-lasting performance and reliability.",
#         "Built to withstand harsh environments, this hinge is designed for durability and longevity.",
#         "Simple installation process, no tools required, making it easy for DIY enthusiasts.",
#     ],
# }

# # Download the dynamic product image
# img = requests.get(product["product_image"]).content
# with open("product.jpg", "wb") as f:
#     f.write(img)

# # Generate explicit structured titles to pair with descriptions for the grid layout
# footer_titles = [
#     "VITAL COMPONENT",
#     "PRECISE COMPATIBILITY",
#     "OEM QUALITY",
#     "DURABLE DESIGN",
# ]

# # Map your list data into specific, short-length roles to avoid text-wrapping issues
# footer_rows = []
# for i in range(4):
#     footer_rows.append(
#         {"title": footer_titles[i], "desc": product["footer_points"][i]}
#     )

# # Dynamic prompt enforcing layout, star counts, and explicit footer grid alignment
# prompt = f"""
# Create a premium, modern, professional Repair Information infographic for an appliance replacement part. The design should look like it was created by a professional graphic designer for a large e-commerce brand such as RepairClinic, PartSelect, GE Appliances, Whirlpool, Samsung Parts, or Sears PartsDirect. The infographic must have a clean, trustworthy, and premium appearance suitable for an online product page.

# Use the following dynamic data exactly as provided:

# Product Name: {{Drum Bearing Sleeve WE1M462}}
# Part Number: {{WE1M462}}
# Product Image: {{https://www.partselect.com/266777-1-M-GE-WE1M462-Drum-Bearing-Sleeve.jpg}}
# Repair Difficulty: {{Easy}}
# DIY Friendly: {{yes}}
# Estimated Repair Time: {{1-2 hours}}
# Safety Tip: {{SAFETY_TIP}}
# Feature 1: {{OEM Quality Assurance: Manufactured to meet strict specifications, ensuring high reliability and long-term durability for your appliance.}}
# Feature 2: {{Designed for Best Fit: Engineered for precise alignment, this part seamlessly integrates into your dryer for optimal performance.}}
# Feature 3: {{Easy DIY Installation: Designed for straightforward replacement to quickly restore your dryer’s operation.}}
# Feature 4: {{Durable Construction: Built to resist wear, this part guarantees dependable operation during everyday use.}}

# The canvas should be 2000 × 2000 pixels in a square format with ultra-high resolution suitable for commercial use.

# Use a white background with subtle blue gradients, soft geometric patterns, faint hexagonal textures, and elegant lighting effects. The design should be minimal, premium, and uncluttered with lots of white space.

# At the top center, display a large bold heading reading "Repair Information" in a modern sans-serif font using a deep blue color. Below it, create a rounded blue pill-shaped banner containing {{PRODUCT_NAME}} – {{PART_NUMBER}} with soft gradients and a subtle shadow.

# Place the supplied {{PRODUCT_IMAGE}} in the center of the infographic. Remove its background completely while preserving the exact product shape and colors. The product must remain unchanged, highly detailed, and photorealistic. Position it inside a large white circular frame with a thin blue border, soft inner glow, and elegant shadow. The product should occupy approximately 45–50% of the total canvas and be the main visual focus.

# On the left side, create two vertically stacked rounded white information cards.

# The first card should contain a blue circular wrench icon at the top, followed by the title Repair Difficulty. Below it, display {{REPAIR_DIFFICULTY}} inside a colored rounded badge. Automatically choose the badge color based on the value:

# Easy → Green
# Medium → Orange
# Hard → Red
# Very Hard → Dark Red

# Below the badge, display a short supporting description appropriate for the difficulty level.

# The second left card should contain a blue circular person-with-checkmark icon. The title should be DIY Friendly. Display {{DIY_FRIENDLY}} inside a colored rounded badge. Automatically choose:

# YES → Green
# NO → Red
# Professional Recommended → Orange
# Professional Only → Dark Red

# Below the badge, display a short one-line explanation.

# On the right side, create two vertically stacked rounded white information cards.

# The first card should contain a blue circular clock icon. Display the title Estimated Repair Time followed by {{ESTIMATED_REPAIR_TIME}} in large bold typography. Underneath, include a small supporting sentence indicating the approximate installation time.

# The second card should contain a blue circular shield icon. Display the title Safety Tip followed by {{SAFETY_TIP}}. The safety text should be clean, easy to read, and limited to a few lines.

# At the bottom of the infographic, create a full-width rounded blue feature bar divided into four equal sections. Each section should include a premium white vector icon, a feature heading, and a short supporting description generated from:

# {{FEATURE_1}}
# {{FEATURE_2}}
# {{FEATURE_3}}
# {{FEATURE_4}}

# Automatically choose suitable icons that match each feature, such as OEM Quality, Reliable Performance, Easy Installation, Perfect Fit, Durable Materials, Leak Resistant, Exact Replacement, or High Performance.

# Use modern premium vector icons throughout the design. All cards should have soft shadows, rounded corners, balanced spacing, and consistent alignment. Typography should be bold, crisp, and easy to read. Maintain a clean visual hierarchy with the product image as the primary focal point.

# The infographic should have a premium OEM branding style with white and blue colors, subtle gradients, soft shadows, rounded elements, and a balanced layout. Everything should appear realistic, polished, and suitable for an appliance parts e-commerce website.

# Do not include customer ratings, review stars, testimonials, review counts, logos, watermarks, QR codes, prices, discount badges, promotional stickers, or any unnecessary decorative elements. Do not invent any information that is not provided in the dynamic data. If any field is missing, automatically adjust the layout so there are no empty sections.

# The final output should be photorealistic, pixel-perfect, professionally designed, commercially usable, and visually indistinguishable from a premium infographic created by an experienced graphic designer for a leading appliance parts brand.
# """

# # Call the image edit API
# result = client.images.edit(
#     model="gpt-image-1",
#     image=[
#         open("sample.png", "rb"),
#         open("product.jpg", "rb"),
#     ],
#     prompt=prompt,
#     size="1024x1024",
# )

# # Decode and save the output image
# image_bytes = base64.b64decode(result.data[0].b64_json)

# with open("output.png", "wb") as f:
#     f.write(image_bytes)

# print("Done")

import base64
import requests
from openai import OpenAI
from shared import API_KEY

client = OpenAI(api_key=API_KEY)

product = {
    "product_name": "GE Refrigerator Top Hinge & Pin (Right)",
    "part_number": "WR13X28532",
    "product_image": "https://cdn.shopify.com/s/files/1/0700/0293/3937/files/GEWR13X28532.jpg?v=1762638428",
    "repair_difficulty": "Easy",
    "estimated_time": "15–30 Minutes",
    "diy": "YES",
    "features": [
        {
            "title": "OEM QUALITY",
            "description": "Manufactured to OEM specifications."
        },
        {
            "title": "PERFECT FIT",
            "description": "Designed for accurate compatibility."
        },
        {
            "title": "EASY INSTALLATION",
            "description": "Quick replacement with basic tools."
        },
        {
            "title": "DURABLE",
            "description": "Built for long lasting performance."
        }
    ]
}

#########################################################
# Generate Safety Tip using GPT-5.5
#########################################################

response = client.responses.create(
    model="gpt-4o",
    input=f"""
You are an appliance repair expert.

Generate ONE short safety tip for replacing this appliance part.

Product:
{product["product_name"]}

Rules:
- Maximum 12 words.
- One sentence.
- Plain English.
- Practical advice.
- No quotation marks.
"""
)

product["safety_tip"] = response.output_text.strip()
print(product["safety_tip"])

#########################################################
# Download Product Image
#########################################################

img = requests.get(product["product_image"]).content

with open("product.jpg", "wb") as f:
    f.write(img)

#########################################################
# Prompt
#########################################################

# Collect every literal string that must appear in the image, verbatim.
# Rendering these as an explicit "TEXT TO RENDER EXACTLY" manifest — instead
# of burying them inside prose — is what stops gpt-image-1 from silently
# "autocorrecting" or reflowing the text and introducing typos.
text_manifest = {
    "HEADER": "Repair Information",
    "PRODUCT_NAME": product["product_name"],
    "PART_NUMBER_LABEL": "Part Number",
    "PART_NUMBER": product["part_number"],
    "CARD1_LABEL": "Repair Difficulty",
    "CARD1_VALUE": product["repair_difficulty"],
    "CARD2_LABEL": "DIY Friendly",
    "CARD2_VALUE": product["diy"],
    "CARD3_LABEL": "Estimated Repair Time",
    "CARD3_VALUE": product["estimated_time"],
    "CARD4_LABEL": "Safety Tip",
    "CARD4_VALUE": product["safety_tip"],
    "FEATURE1_TITLE": product["features"][0]["title"],
    "FEATURE1_DESC": product["features"][0]["description"],
    "FEATURE2_TITLE": product["features"][1]["title"],
    "FEATURE2_DESC": product["features"][1]["description"],
    "FEATURE3_TITLE": product["features"][2]["title"],
    "FEATURE3_DESC": product["features"][2]["description"],
    "FEATURE4_TITLE": product["features"][3]["title"],
    "FEATURE4_DESC": product["features"][3]["description"],
}

manifest_lines = ""

for key, value in text_manifest.items():
    manifest_lines += f"""
==============================
TEXT_ID: {key}
COPY EXACTLY:
{value}
==============================

"""
prompt = f"""
IMPORTANT:

Use sample.png as the EXACT design template.

This is an EDITING task.

Do NOT redesign anything.

Do NOT create a new layout.

Keep EXACTLY:

• Same typography
• Same font sizes
• Same spacing
• Same card sizes
• Same icons
• Same shadows
• Same colors
• Same footer
• Same white background
• Same circular product frame
• Same composition

Only replace the existing content.

------------------------------------------------

TEXT TO RENDER — COPY EACH STRING EXACTLY, CHARACTER BY CHARACTER

The strings below are the ONLY text allowed in the image. Treat each one as
a literal string to be copied, not a sentence to be rephrased, summarized,
autocorrected, or "improved". Reproduce spelling, capitalization, spacing,
and punctuation exactly as written between the quotation marks. Do not add,
remove, or substitute any letters.

{manifest_lines}

Placement:
- [HEADER] -> top header
- [PRODUCT_NAME] and [PART_NUMBER_LABEL]/[PART_NUMBER] -> product info block
- [CARD1_LABEL]/[CARD1_VALUE] -> left card 1 (use a green badge if the value is "Easy")
- [CARD2_LABEL]/[CARD2_VALUE] -> left card 2 (use a green badge if the value is "YES")
- [CARD3_LABEL]/[CARD3_VALUE] -> right card 1
- [CARD4_LABEL]/[CARD4_VALUE] -> right card 2
- [FEATURE1_TITLE]/[FEATURE1_DESC] through [FEATURE4_TITLE]/[FEATURE4_DESC] -> the four bottom feature blocks, in order

------------------------------------------------

PRODUCT IMAGE

Replace the existing product photo with product.jpg.

Remove its background.

Do not modify the product.

Do not rotate it.

Do not crop it.

------------------------------------------------

CRITICAL INSTRUCTIONS

THIS IS NOT A TEXT GENERATION TASK.

THIS IS A TEXT COPYING TASK.

The text provided above is FINAL.

Every string inside the manifest is immutable.

Copy every character exactly as provided.

Treat every letter as locked.

Treat every word as locked.

Treat every sentence as locked.

Do NOT rewrite.

Do NOT paraphrase.

Do NOT summarize.

Do NOT improve wording.

Do NOT autocorrect.

Do NOT fix grammar.

Do NOT fix spelling.

Do NOT replace words.

Do NOT replace punctuation.

Do NOT replace symbols.

Do NOT replace numbers.

Do NOT change capitalization.

Do NOT change spaces.

Do NOT change hyphens.

Do NOT change brackets.

Do NOT change quotation marks.

Do NOT add periods.

Do NOT remove periods.

Do NOT insert commas.

Do NOT remove commas.

Do NOT insert additional text.

Do NOT remove any text.

Do NOT translate.

Do NOT abbreviate.

Do NOT expand abbreviations.

Do NOT generate placeholder text.

Do NOT generate lorem ipsum.

Do NOT generate gibberish.

Do NOT hallucinate.

Do NOT guess.

Do NOT infer missing text.

Every rendered character must match the manifest exactly.

Render text character-by-character.

Letter-by-letter.

Word-by-word.

Line-by-line.

Examples:

Input:
OEM QUALITY

Output:
OEM QUALITY

NOT:
OEM Qualty
OEM Quality
OEM-QUALITY
OEM Quality Assurance

--------------------------------

Input:
WR13X28532

Output:
WR13X28532

NOT:
WR13X28523
WR13X2853Z
WR13X-28532

--------------------------------

Input:
15–30 Minutes

Output:
15–30 Minutes

NOT:
15-30 Minutes
15 to 30 Minutes
15–30 mins

--------------------------------

Input:
DIY Friendly

Output:
DIY Friendly

NOT:
DIY friendy
DIY Friendly?
DIY-Friendly

--------------------------------

If any text does not fit,
reduce ONLY the font size.

Never crop text.

Never truncate text.

Never wrap words incorrectly.

Never merge words.

Never split words.

Never replace characters with similar-looking ones.

Every text box must contain ONLY the exact text from the manifest.

No spelling mistakes.

No OCR artifacts.

No invented letters.

No missing letters.

No extra letters.

Before generating the final image, compare every rendered string with the manifest.

If even one character differs, replace it with the exact original character.

The final image should contain ZERO spelling mistakes.

The text must be an exact visual copy of the manifest.

------------------------------------------------
"""

#########################################################
# Image Edit
#########################################################

result = client.images.edit(
    model="gpt-image-1",
    image=[
        open("sample.png", "rb"),
        open("product.jpg", "rb"),
    ],
    prompt=prompt,
    size="1024x1024",
)

#########################################################
# Save
#########################################################

image = base64.b64decode(result.data[0].b64_json)

with open("output.png", "wb") as f:
    f.write(image)

print("Done!")