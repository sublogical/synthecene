import os
import base64
import json
from dataclasses import dataclass
from anthropic import Anthropic
import argparse
from enum import Enum

@dataclass
class ModelDefinition:
    model_spec: str
    model_lineage: list[str]
    model_origin: str

ANTHROPIC_CLAUDE_3_5_SONNET_LATEST = ModelDefinition(
    model_spec="claude-3-5-sonnet-latest",
    model_lineage=["claude-3-5-sonnet", "claude-3-5", "claude-3"],
    model_origin="anthropic",
)

MODEL_CATALOG = [
    ANTHROPIC_CLAUDE_3_5_SONNET_LATEST
]

SYSTEM_PROMPT_CATALOG = {
    "image_critic_system_prompt": {
        "default": "You are a seasoned art critic at a respected art publication. You have perfect vision and pay great attention to detail which makes you an expert at analyzing images. Before providing the answer in <answer> tags, think step by step in <thinking> tags and analyze every part of the image."
    },
    "general_image_classification": {
        "default": "Analyze this image and output in JSON format with keys: {JSON_SCHEMA}"
    }
}
BASIC_IMAGE_CLASSIFICATION_KEYS = {
    "image-type": {
        "description": "The type of image",
        "type": "string",
        "values": ["photo", "illustration", "graphic", "painting", "other"]
    },
    "caption-short": {
        "description": "A short caption for the image, capturing the main subject and action. It should be no more than 10 words.",
        "type": "string"
    },
    "caption-long": {
        "description": "A long caption for the image, capturing sufficient detail to answer questions accurately about the image, and to recreate the image as accurately as possible using generative AI. Minimum 100 words, maximum 200 words.",
        "type": "string"
    },
    "color-palette": {
        "description": "A list of colors in the image, sorted by frequency of appearance. Use standard color names from CSS. The list should be no more than 10 colors.",
        "type": "string"
    },
    "topics": {
        "description": "A list of topics in the image, sorted by frequency of appearance. The list should be no more than 10 topics.",
        "type": "list",
        "values": ["landscape", "portrait", "animal", "human", "object", "other"]
    },
    "composition_score": {
        "description": "The score for the composition of the image",
        "type": "number",
        "range": [0, 100]
    },
    "overall_quality": {
        "description": "The score for the overall quality of the image",
        "type": "number",
        "range": [0, 100]
    },
}

IMAGE_PII_KEYS = {
    "contains_face": {
        "description": "Whether the image contains a face",
        "type": "boolean"
    },
    "number_of_people": {
        "description": "The number of people in the image",
        "type": "number"
    },
    "contains_child": {
        "description": "Whether the image contains a child",
        "type": "boolean"
    },
}

IMAGE_SENSITIVITY_KEYS = {
    "adultness": {
        "description": "The score for the adultness of the image",
        "type": "number",
        "range": [0, 100]
    },
    "contains_nudity": {
        "description": "Whether the image contains nudity",
        "type": "boolean"
    },
    "contains_violence": {
        "description": "Whether the image contains violence",
        "type": "boolean"
    },
    "contains_drugs": {
        "description": "Whether the image contains drugs",
        "type": "boolean"
    },
    "contains_alcohol": {
        "description": "Whether the image contains alcohol",
        "type": "boolean"
    },
    "contains_smoking": {
        "description": "Whether the image contains smoking",
        "type": "boolean"
    },
    
}

CLASSIFICATION_CATALOG = {
    "basic_image_classification": {
        "description": "Basic image classification",
        "keys": BASIC_IMAGE_CLASSIFICATION_KEYS
    },
    "image_pii": {
        "description": "Image PII",
        "keys": IMAGE_PII_KEYS
    },
    "image_sensitivity": {
        "description": "Image sensitivity",
        "keys": IMAGE_SENSITIVITY_KEYS
    }
}

def get_classification_set(classification_set_name: str) -> dict:
    if classification_set_name not in CLASSIFICATION_CATALOG:
        raise ValueError(f"Classification set {classification_set_name} not found in {CLASSIFICATION_CATALOG.keys()}")
    return CLASSIFICATION_CATALOG[classification_set_name]["keys"]

def get_json_description(schema: dict):
    # return a string that describes the JSON schema in a form the LLM can understand
    descriptions = []

    for key, value in schema.items():
        description = f"{key}: {value['description']}"
        
        if value["type"] == "string":
            description += f" (string"
            if "values" in value:
                description += f", allowed-values: {', '.join(value['values'])}"
            description += ")"
        elif value["type"] == "number":
            description += f" (number"
            if "range" in value:
                description += f", range: {value['range'][0]} to {value['range'][1]}"
            description += ")"
            
        descriptions.append(description)

    return ", ".join(descriptions)

def get_prompt(model: ModelDefinition, prompt_type: str):
    if prompt_type not in SYSTEM_PROMPT_CATALOG:
        raise ValueError(f"Prompt type {prompt_type} not found in {SYSTEM_PROMPT_CATALOG.keys()}")

    if model.model_spec in SYSTEM_PROMPT_CATALOG[prompt_type]:
        return SYSTEM_PROMPT_CATALOG[prompt_type][model.model_spec]

    for model_spec in model.model_lineage:
        if model_spec in SYSTEM_PROMPT_CATALOG[prompt_type]:
            return SYSTEM_PROMPT_CATALOG[prompt_type][model_spec]

    return SYSTEM_PROMPT_CATALOG[prompt_type]["default"]

MODEL_NAME = "claude-3-opus-20240229"

def get_base64_encoded_image(image_path):
    with open(image_path, "rb") as image_file:
        binary_data = image_file.read()
        base_64_encoded_data = base64.b64encode(binary_data)
        base64_string = base_64_encoded_data.decode('utf-8')
        return base64_string

class LabelObjectType(Enum):
    IMAGE = 1
    TEXT = 2
    VIDEO = 3

@dataclass
class LabelObject:
    object_type: LabelObjectType
    object_path: str

def autolabel_anthropic(label_objects: list[LabelObject], model: ModelDefinition, prompt_type: str, classification_set: dict):
    # get credentials from env
    client = Anthropic(
        api_key=os.environ.get("ANTHROPIC_API_KEY"),  # This is the default and can be omitted
    )

    content = []
    for label_object in label_objects:
        if label_object.object_type == LabelObjectType.IMAGE:
            content.append({"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": get_base64_encoded_image(label_object.object_path)}})
        elif label_object.object_type == LabelObjectType.TEXT:
            content.append({"type": "text", "text": label_object.object_path})

    json_schema = get_json_description(classification_set)
    prompt = get_prompt(model, prompt_type)
    prompt = prompt.replace("{JSON_SCHEMA}", json_schema)
    
    content.append({"type": "text", "text": prompt})

    response = client.messages.create(
        max_tokens=2048,
        messages=[
            {
                "role": "user",
                "content": content
            }
        ],
        model=model.model_spec,
        system=get_prompt(model, "image_critic_system_prompt"),
    )

    response_text = response.content[0].text
    return parse_anthropic_response(response_text)

def parse_anthropic_response(response: str) -> dict:
    # parse the response into a dictionary
    # the response is in the format of <thinking> and <answer> tags
    # the <answer> tag contains the JSON output
    # the <thinking> tag contains the reasoning
    # return the dictionary
    answer_start = response.find("<answer>") + len("<answer>")
    answer_end = response.find("</answer>")
    answer = response[answer_start:answer_end].strip()
    return json.loads(answer)

def autolabel_gemini(label_objects: list[LabelObject]):
    pass

def autolabel_grok(label_objects: list[LabelObject]):
    pass

def get_label_objects(args: argparse.Namespace) -> list[LabelObject]:
    if args.image_path:
        return [LabelObject(LabelObjectType.IMAGE, args.image_path)]
    elif args.image_url:
        raise ValueError("Image URL not supported yet")
    elif args.text:
        return [LabelObject(LabelObjectType.TEXT, args.text)]
    elif args.video_path:
        return [LabelObject(LabelObjectType.VIDEO, args.video_path)]
    elif args.video_url:
        raise ValueError("Video URL not supported yet")

def get_model(model_name: str) -> ModelDefinition:
    for model in MODEL_CATALOG:
        if model.model_spec == model_name:
            return model
    raise ValueError(f"Model {model_name} not found")

def autolabel(label_objects: list[LabelObject], model: ModelDefinition, prompt_type: str, classification_set: dict):
    if model.model_origin == "anthropic":
        return autolabel_anthropic(label_objects, model, prompt_type, classification_set)
    elif model.model_origin == "gemini":
        return autolabel_gemini(label_objects)
    elif model.model_origin == "grok":
        return autolabel_grok(label_objects)
    else:
        raise ValueError(f"Model origin {model.model_origin} not supported")
   
if __name__ == "__main__":
    # parse args
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--image_path', action="store", type=str, help="Path to the image to analyze")
    group.add_argument('--image_url', action="store", type=str, help="URL of the image to analyze")
    group.add_argument('--text', action="store", type=str, help="Text to analyze")
    group.add_argument('--video_path', action="store", type=str, help="Path to the video to analyze")
    group.add_argument('--video_url', action="store", type=str, help="URL of the video to analyze")
    parser.add_argument("--model", type=str, required=True)
    parser.add_argument("--classification_set", type=str, required=True)
    args = parser.parse_args()

    label_objects = get_label_objects(args)

    model = get_model(args.model)
    if model is None:
        model = ANTHROPIC_CLAUDE_3_5_SONNET_LATEST

    classification_set = get_classification_set(args.classification_set)
    prompt_type = "general_image_classification"

    response = autolabel(label_objects, model, prompt_type, classification_set)
    print(json.dumps(response, indent=4))


def test_get_json_description():
    assert get_json_description(IMAGE_CLASSIFICATION_KEYS) == "image-type: The type of image (string, allowed-values: photo, illustration, other), caption-short: A short caption for the image (string), caption-long: A long caption for the image (string), composition_score: The score for the composition of the image (number, range: 0 to 100), overall_quality: The score for the overall quality of the image (number, range: 0 to 100), adultness: The score for the adultness of the image (number, range: 0 to 100)"

def test_get_prompt():
    assert get_prompt(ANTHROPIC_CLAUDE_3_5_SONNET_LATEST, "general_image_classification") == "Analyze this image and output in JSON format with keys: {JSON_SCHEMA}"


def test_parse_anthropic_response():
    raw_response = """
<thinking>
Let me analyze this image step by step:

1. Image Type Assessment:
- This is clearly a photograph
- Shot during golden hour/sunset
- Natural lighting conditions

2. Composition Analysis:
- Strong leading lines with the wooden pier/dock
- Rule of thirds followed with subjects positioned on right third
- Beautiful symmetry with the pier leading to horizon
- Excellent depth perspective
- Warm, golden tones create mood

3. Technical Quality:
- Sharp focus
- Good exposure balance
- Beautiful backlighting
- Slight haze adds atmosphere
- High resolution image

4. Content Analysis:
- Family moment captured
- Wholesome subject matter
- Appropriate for all audiences
- Tells a story of connection

5. Artistic Merit:
- Strong emotional resonance
- Excellent use of natural light
- Professional-level composition
- Memorable moment captured
</thinking>

<answer>
{
    "image-type": "photo",
    "caption-short": "Mother and child walking on pier at sunset",
    "caption-long": "A serene moment captured during golden hour shows silhouettes of a parent and child walking hand in hand along a wooden pier, set against a warm, glowing sky. The composition creates a powerful sense of journey, connection, and peaceful contemplation.",
    "composition_score": 95,
    "overall_quality": 92,
    "adultness": 0
}
</answer>    
"""
    assert parse_anthropic_response(raw_response) == {
        "image-type": "photo",
        "caption-short": "Mother and child walking on pier at sunset",
        "caption-long": "A serene moment captured during golden hour shows silhouettes of a parent and child walking hand in hand along a wooden pier, set against a warm, glowing sky. The composition creates a powerful sense of journey, connection, and peaceful contemplation.",
        "composition_score": 95,
        "overall_quality": 92,
        "adultness": 0
    }
