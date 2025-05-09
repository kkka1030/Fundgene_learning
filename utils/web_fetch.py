import requests
from requests.exceptions import RequestException
from typing import Optional, Dict, Any, Tuple
from bs4 import BeautifulSoup

def fetch_text_from_url(url: str, 
                       target_selector: Optional[str] = None,
                       headers: Optional[Dict[str, str]] = None, 
                       timeout: int = 30, 
                       verify_ssl: bool = True,
                       maxlen: int = 1000000) -> str:
    """
    从指定URL获取提取后的文本内容
    
    参数:
        url (str): 要获取内容的网页URL
        target_selector (str, optional): 目标内容的CSS选择器，如果提供，将只提取该选择器匹配的内容
        headers (dict, optional): 请求头信息，默认为模拟常规浏览器
        timeout (int): 请求超时时间（秒），默认30秒
        verify_ssl (bool): 是否验证SSL证书，默认为True
        maxlen (int): 提取文本的最大长度，默认1000000字符，请不要超过模型最大token数的2/3
        
    返回:
        str: 提取的文本内容或错误信息
    """
    # 默认请求头
    default_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8'
    }
    
    # 使用提供的请求头或默认请求头
    request_headers = headers if headers else default_headers
    
    try:
        # 发送GET请求
        response = requests.get(
            url, 
            headers=request_headers, 
            timeout=timeout,
            verify=verify_ssl
        )
        
        # 检查响应状态码
        response.raise_for_status()
        
        # 获取HTML内容
        html_content = response.text
        
        try:
            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 如果提供了选择器，则只提取选择器匹配的内容
            if target_selector:
                target_elements = soup.select(target_selector)
                if target_elements:
                    # 创建新的BeautifulSoup对象只包含目标元素
                    soup = BeautifulSoup('', 'html.parser')
                    for element in target_elements:
                        soup.append(element)
            
            # 移除脚本和样式元素
            for script_or_style in soup(['script', 'style', 'head', 'title', 'meta', '[document]', 'footer', 'header', 'nav']):
                script_or_style.decompose()
                
            # 提取所有文本
            text = soup.get_text()
            
            # 处理文本，清理多余空白
            lines = (line.strip() for line in text.splitlines())
            chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
            text = '\n'.join(chunk for chunk in chunks if chunk)
            
            return text[:maxlen]
            
        except Exception as e:
            return f"解析HTML失败: {str(e)}"
            
    except RequestException as e:
        return f"请求错误: {str(e)}"
    except Exception as e:
        return f"未知错误: {str(e)}"


# 使用示例
if __name__ == "__main__":
    url = "https://edition.cnn.com/2025/05/02/australia/polling-young-voters-australia-election-intl-hnk"
    
    # 先尝试使用特定选择器提取CNN文章内容（CNN常用的内容选择器）
    cnn_selector = "div.article__content, div.article-content, div.l-container"
    text_content = fetch_text_from_url(url, target_selector=cnn_selector)
    
    # 如果使用选择器提取结果很少，则尝试全局提取
    if "解析HTML失败" in text_content or len(text_content.strip()) < 100:
        print("使用选择器提取失败，尝试全局提取...")
        text_content = fetch_text_from_url(url)
    
    print("\n提取的文本内容:")
    print(text_content)