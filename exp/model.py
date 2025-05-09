GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")

model_client = OpenAIChatCompletionClient(
    model="gemini-2.0-flash", 
    base_url="https://generativelanguage.googleapis.com/v1beta/openai/",
    api_key=GEMINI_API_KEY, 
    model_info={
        "vision": False,
        "function_calling": True,
        "json_output": True,
        "family": "gemini",
    },
)
