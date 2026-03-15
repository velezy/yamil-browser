"""
API Documentation Generation for Logic Weaver.

Provides comprehensive API documentation features:
- Auto-generated OpenAPI 3.0/3.1 specifications
- Interactive Swagger UI and ReDoc integration
- SDK generation for multiple languages
- Code samples generation
- Markdown documentation export

Comparable to Apigee's Developer Portal and AWS API Gateway documentation.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Union
import json
import re
import logging

logger = logging.getLogger(__name__)


class OpenAPIVersion(str, Enum):
    """Supported OpenAPI versions."""
    V3_0 = "3.0.3"
    V3_1 = "3.1.0"


class SDKLanguage(str, Enum):
    """Supported SDK languages."""
    PYTHON = "python"
    JAVASCRIPT = "javascript"
    TYPESCRIPT = "typescript"
    JAVA = "java"
    CSHARP = "csharp"
    GO = "go"
    RUBY = "ruby"
    PHP = "php"
    SWIFT = "swift"
    KOTLIN = "kotlin"
    RUST = "rust"


class DocumentationFormat(str, Enum):
    """Documentation export formats."""
    OPENAPI_JSON = "openapi_json"
    OPENAPI_YAML = "openapi_yaml"
    MARKDOWN = "markdown"
    HTML = "html"
    POSTMAN = "postman"
    INSOMNIA = "insomnia"


@dataclass
class APIContact:
    """API contact information."""
    name: str = ""
    email: str = ""
    url: str = ""

    def to_dict(self) -> Dict[str, str]:
        result = {}
        if self.name:
            result["name"] = self.name
        if self.email:
            result["email"] = self.email
        if self.url:
            result["url"] = self.url
        return result


@dataclass
class APILicense:
    """API license information."""
    name: str = "Apache 2.0"
    url: str = "https://www.apache.org/licenses/LICENSE-2.0"
    identifier: str = ""  # SPDX identifier for OpenAPI 3.1

    def to_dict(self) -> Dict[str, str]:
        result = {"name": self.name}
        if self.url:
            result["url"] = self.url
        if self.identifier:
            result["identifier"] = self.identifier
        return result


@dataclass
class APIInfo:
    """API metadata for documentation."""
    title: str
    version: str
    description: str = ""
    terms_of_service: str = ""
    contact: APIContact = field(default_factory=APIContact)
    license: APILicense = field(default_factory=APILicense)

    # Extended info
    logo_url: str = ""
    external_docs_url: str = ""
    external_docs_description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "title": self.title,
            "version": self.version,
        }
        if self.description:
            result["description"] = self.description
        if self.terms_of_service:
            result["termsOfService"] = self.terms_of_service
        if self.contact.name or self.contact.email or self.contact.url:
            result["contact"] = self.contact.to_dict()
        if self.license.name:
            result["license"] = self.license.to_dict()
        return result


@dataclass
class ServerVariable:
    """OpenAPI server variable."""
    default: str
    enum: List[str] = field(default_factory=list)
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = {"default": self.default}
        if self.enum:
            result["enum"] = self.enum
        if self.description:
            result["description"] = self.description
        return result


@dataclass
class Server:
    """OpenAPI server definition."""
    url: str
    description: str = ""
    variables: Dict[str, ServerVariable] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        result = {"url": self.url}
        if self.description:
            result["description"] = self.description
        if self.variables:
            result["variables"] = {k: v.to_dict() for k, v in self.variables.items()}
        return result


@dataclass
class SecurityScheme:
    """OpenAPI security scheme."""
    type: str  # apiKey, http, oauth2, openIdConnect
    name: str = ""  # For apiKey
    location: str = ""  # header, query, cookie for apiKey
    scheme: str = ""  # For http (bearer, basic)
    bearer_format: str = ""  # For http bearer
    flows: Dict[str, Any] = field(default_factory=dict)  # For oauth2
    openid_connect_url: str = ""  # For openIdConnect
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = {"type": self.type}
        if self.description:
            result["description"] = self.description
        if self.type == "apiKey":
            result["name"] = self.name
            result["in"] = self.location
        elif self.type == "http":
            result["scheme"] = self.scheme
            if self.bearer_format:
                result["bearerFormat"] = self.bearer_format
        elif self.type == "oauth2":
            result["flows"] = self.flows
        elif self.type == "openIdConnect":
            result["openIdConnectUrl"] = self.openid_connect_url
        return result


@dataclass
class Tag:
    """OpenAPI tag for grouping endpoints."""
    name: str
    description: str = ""
    external_docs_url: str = ""
    external_docs_description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        result = {"name": self.name}
        if self.description:
            result["description"] = self.description
        if self.external_docs_url:
            result["externalDocs"] = {
                "url": self.external_docs_url,
            }
            if self.external_docs_description:
                result["externalDocs"]["description"] = self.external_docs_description
        return result


@dataclass
class CodeSample:
    """Code sample for an API endpoint."""
    language: str
    label: str
    source: str

    def to_dict(self) -> Dict[str, str]:
        return {
            "lang": self.language,
            "label": self.label,
            "source": self.source,
        }


@dataclass
class EndpointDoc:
    """Documentation for a single endpoint."""
    path: str
    method: str
    summary: str = ""
    description: str = ""
    operation_id: str = ""
    tags: List[str] = field(default_factory=list)
    deprecated: bool = False
    security: List[Dict[str, List[str]]] = field(default_factory=list)
    code_samples: List[CodeSample] = field(default_factory=list)

    # Request/Response info (typically auto-generated)
    request_body: Optional[Dict[str, Any]] = None
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    responses: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class OpenAPIGenerator:
    """
    Generates OpenAPI specifications from FastAPI apps or manual definitions.

    Features:
    - OpenAPI 3.0 and 3.1 support
    - Security scheme definitions
    - Server configurations
    - Tag grouping
    - Code samples integration (x-codeSamples)
    """

    def __init__(
        self,
        info: APIInfo,
        version: OpenAPIVersion = OpenAPIVersion.V3_1,
    ):
        self.info = info
        self.version = version
        self.servers: List[Server] = []
        self.security_schemes: Dict[str, SecurityScheme] = {}
        self.tags: List[Tag] = []
        self.paths: Dict[str, Dict[str, Any]] = {}
        self.components: Dict[str, Any] = {"schemas": {}, "securitySchemes": {}}
        self.global_security: List[Dict[str, List[str]]] = []

    def add_server(self, server: Server) -> None:
        """Add a server definition."""
        self.servers.append(server)

    def add_security_scheme(self, name: str, scheme: SecurityScheme) -> None:
        """Add a security scheme."""
        self.security_schemes[name] = scheme
        self.components["securitySchemes"][name] = scheme.to_dict()

    def add_tag(self, tag: Tag) -> None:
        """Add a tag for grouping endpoints."""
        self.tags.append(tag)

    def set_global_security(self, security: List[Dict[str, List[str]]]) -> None:
        """Set global security requirements."""
        self.global_security = security

    def add_schema(self, name: str, schema: Dict[str, Any]) -> None:
        """Add a component schema."""
        self.components["schemas"][name] = schema

    def add_endpoint(self, endpoint: EndpointDoc) -> None:
        """Add an endpoint to the spec."""
        if endpoint.path not in self.paths:
            self.paths[endpoint.path] = {}

        operation = {}
        if endpoint.summary:
            operation["summary"] = endpoint.summary
        if endpoint.description:
            operation["description"] = endpoint.description
        if endpoint.operation_id:
            operation["operationId"] = endpoint.operation_id
        if endpoint.tags:
            operation["tags"] = endpoint.tags
        if endpoint.deprecated:
            operation["deprecated"] = True
        if endpoint.security:
            operation["security"] = endpoint.security
        if endpoint.parameters:
            operation["parameters"] = endpoint.parameters
        if endpoint.request_body:
            operation["requestBody"] = endpoint.request_body
        if endpoint.responses:
            operation["responses"] = endpoint.responses
        else:
            operation["responses"] = {"200": {"description": "Successful response"}}

        # Add code samples as x-codeSamples extension
        if endpoint.code_samples:
            operation["x-codeSamples"] = [s.to_dict() for s in endpoint.code_samples]

        self.paths[endpoint.path][endpoint.method.lower()] = operation

    def generate(self) -> Dict[str, Any]:
        """Generate the complete OpenAPI specification."""
        spec = {
            "openapi": self.version.value,
            "info": self.info.to_dict(),
        }

        if self.servers:
            spec["servers"] = [s.to_dict() for s in self.servers]

        if self.tags:
            spec["tags"] = [t.to_dict() for t in self.tags]

        if self.paths:
            spec["paths"] = self.paths

        if self.components["schemas"] or self.components["securitySchemes"]:
            spec["components"] = self.components

        if self.global_security:
            spec["security"] = self.global_security

        if self.info.external_docs_url:
            spec["externalDocs"] = {
                "url": self.info.external_docs_url,
            }
            if self.info.external_docs_description:
                spec["externalDocs"]["description"] = self.info.external_docs_description

        return spec

    def to_json(self, indent: int = 2) -> str:
        """Export as JSON string."""
        return json.dumps(self.generate(), indent=indent)

    def to_yaml(self) -> str:
        """Export as YAML string."""
        try:
            import yaml
            return yaml.dump(self.generate(), default_flow_style=False, sort_keys=False)
        except ImportError:
            logger.warning("PyYAML not installed, falling back to JSON")
            return self.to_json()


class CodeSampleGenerator:
    """
    Generates code samples for API endpoints.

    Supports multiple languages with idiomatic patterns.
    """

    def __init__(self, base_url: str = "https://api.example.com"):
        self.base_url = base_url

    def generate(
        self,
        endpoint: EndpointDoc,
        language: SDKLanguage,
    ) -> CodeSample:
        """Generate a code sample for an endpoint."""
        generators = {
            SDKLanguage.PYTHON: self._generate_python,
            SDKLanguage.JAVASCRIPT: self._generate_javascript,
            SDKLanguage.TYPESCRIPT: self._generate_typescript,
            SDKLanguage.JAVA: self._generate_java,
            SDKLanguage.GO: self._generate_go,
            SDKLanguage.CSHARP: self._generate_csharp,
            SDKLanguage.RUBY: self._generate_ruby,
            SDKLanguage.PHP: self._generate_php,
            SDKLanguage.SWIFT: self._generate_swift,
            SDKLanguage.KOTLIN: self._generate_kotlin,
            SDKLanguage.RUST: self._generate_rust,
        }

        generator = generators.get(language, self._generate_curl)
        return generator(endpoint)

    def generate_all(self, endpoint: EndpointDoc) -> List[CodeSample]:
        """Generate code samples for all supported languages."""
        samples = []
        for lang in SDKLanguage:
            samples.append(self.generate(endpoint, lang))
        # Add curl as first sample
        samples.insert(0, self._generate_curl(endpoint))
        return samples

    def _generate_curl(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate curl command."""
        url = f"{self.base_url}{endpoint.path}"
        cmd = f'curl -X {endpoint.method.upper()} "{url}"'

        if endpoint.security:
            cmd += ' \\\n  -H "Authorization: Bearer $TOKEN"'

        if endpoint.request_body:
            cmd += ' \\\n  -H "Content-Type: application/json"'
            cmd += " \\\n  -d '{\"key\": \"value\"}'"

        return CodeSample(
            language="shell",
            label="cURL",
            source=cmd,
        )

    def _generate_python(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate Python code sample using requests."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.lower()

        code = f'''import requests

url = "{url}"
headers = {{"Authorization": "Bearer YOUR_TOKEN"}}
'''

        if endpoint.request_body:
            code += '''data = {"key": "value"}

response = requests.{method}(url, headers=headers, json=data)
'''.format(method=method)
        else:
            code += f'''
response = requests.{method}(url, headers=headers)
'''

        code += '''print(response.json())'''

        return CodeSample(
            language="python",
            label="Python",
            source=code,
        )

    def _generate_javascript(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate JavaScript code sample using fetch."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.upper()

        code = f'''const response = await fetch("{url}", {{
  method: "{method}",
  headers: {{
    "Authorization": "Bearer YOUR_TOKEN",
    "Content-Type": "application/json"
  }}'''

        if endpoint.request_body:
            code += ''',
  body: JSON.stringify({ key: "value" })'''

        code += '''
});

const data = await response.json();
console.log(data);'''

        return CodeSample(
            language="javascript",
            label="JavaScript",
            source=code,
        )

    def _generate_typescript(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate TypeScript code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.upper()

        code = f'''interface Response {{
  // Define your response type
  data: unknown;
}}

const response = await fetch("{url}", {{
  method: "{method}",
  headers: {{
    "Authorization": "Bearer YOUR_TOKEN",
    "Content-Type": "application/json"
  }}'''

        if endpoint.request_body:
            code += ''',
  body: JSON.stringify({ key: "value" })'''

        code += '''
});

const data: Response = await response.json();
console.log(data);'''

        return CodeSample(
            language="typescript",
            label="TypeScript",
            source=code,
        )

    def _generate_java(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate Java code sample using HttpClient."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.upper()

        code = f'''import java.net.http.*;
import java.net.URI;

HttpClient client = HttpClient.newHttpClient();
HttpRequest request = HttpRequest.newBuilder()
    .uri(URI.create("{url}"))
    .header("Authorization", "Bearer YOUR_TOKEN")
    .header("Content-Type", "application/json")
    .{method.lower()}('''

        if endpoint.request_body:
            code += 'HttpRequest.BodyPublishers.ofString("{\\"key\\": \\"value\\"}"))'
        else:
            code += ")"

        code += '''
    .build();

HttpResponse<String> response = client.send(request,
    HttpResponse.BodyHandlers.ofString());
System.out.println(response.body());'''

        return CodeSample(
            language="java",
            label="Java",
            source=code,
        )

    def _generate_go(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate Go code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.upper()

        code = f'''package main

import (
    "fmt"
    "net/http"
    "io/ioutil"
'''
        if endpoint.request_body:
            code += '    "strings"\n'

        code += f''')

func main() {{
'''
        if endpoint.request_body:
            code += f'''    body := strings.NewReader(`{{"key": "value"}}`)
    req, _ := http.NewRequest("{method}", "{url}", body)
'''
        else:
            code += f'''    req, _ := http.NewRequest("{method}", "{url}", nil)
'''

        code += '''    req.Header.Set("Authorization", "Bearer YOUR_TOKEN")
    req.Header.Set("Content-Type", "application/json")

    client := &http.Client{}
    resp, _ := client.Do(req)
    defer resp.Body.Close()

    data, _ := ioutil.ReadAll(resp.Body)
    fmt.Println(string(data))
}'''

        return CodeSample(
            language="go",
            label="Go",
            source=code,
        )

    def _generate_csharp(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate C# code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method

        code = f'''using System.Net.Http;
using System.Net.Http.Headers;

var client = new HttpClient();
client.DefaultRequestHeaders.Authorization =
    new AuthenticationHeaderValue("Bearer", "YOUR_TOKEN");
'''

        if endpoint.request_body:
            code += f'''
var content = new StringContent(
    @"{{\""key\"": \""value\""}}",
    System.Text.Encoding.UTF8,
    "application/json"
);
var response = await client.{method.capitalize()}Async("{url}", content);
'''
        else:
            code += f'''
var response = await client.{method.capitalize()}Async("{url}");
'''

        code += '''var data = await response.Content.ReadAsStringAsync();
Console.WriteLine(data);'''

        return CodeSample(
            language="csharp",
            label="C#",
            source=code,
        )

    def _generate_ruby(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate Ruby code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.lower()

        code = f'''require 'net/http'
require 'json'

uri = URI("{url}")
http = Net::HTTP.new(uri.host, uri.port)
http.use_ssl = true

request = Net::HTTP::{method.capitalize()}.new(uri)
request["Authorization"] = "Bearer YOUR_TOKEN"
request["Content-Type"] = "application/json"
'''

        if endpoint.request_body:
            code += '''request.body = { key: "value" }.to_json
'''

        code += '''
response = http.request(request)
puts JSON.parse(response.body)'''

        return CodeSample(
            language="ruby",
            label="Ruby",
            source=code,
        )

    def _generate_php(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate PHP code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.upper()

        code = f'''<?php
$curl = curl_init();

curl_setopt_array($curl, [
    CURLOPT_URL => "{url}",
    CURLOPT_RETURNTRANSFER => true,
    CURLOPT_CUSTOMREQUEST => "{method}",
    CURLOPT_HTTPHEADER => [
        "Authorization: Bearer YOUR_TOKEN",
        "Content-Type: application/json"
    ]'''

        if endpoint.request_body:
            code += ''',
    CURLOPT_POSTFIELDS => json_encode(["key" => "value"])'''

        code += '''
]);

$response = curl_exec($curl);
curl_close($curl);

echo $response;
?>'''

        return CodeSample(
            language="php",
            label="PHP",
            source=code,
        )

    def _generate_swift(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate Swift code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.upper()

        code = f'''import Foundation

let url = URL(string: "{url}")!
var request = URLRequest(url: url)
request.httpMethod = "{method}"
request.setValue("Bearer YOUR_TOKEN", forHTTPHeaderField: "Authorization")
request.setValue("application/json", forHTTPHeaderField: "Content-Type")
'''

        if endpoint.request_body:
            code += '''
let body = ["key": "value"]
request.httpBody = try? JSONSerialization.data(withJSONObject: body)
'''

        code += '''
let task = URLSession.shared.dataTask(with: request) { data, response, error in
    if let data = data {
        print(String(data: data, encoding: .utf8)!)
    }
}
task.resume()'''

        return CodeSample(
            language="swift",
            label="Swift",
            source=code,
        )

    def _generate_kotlin(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate Kotlin code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.toLowerCase() if hasattr(endpoint.method, 'toLowerCase') else endpoint.method.lower()

        code = f'''import okhttp3.*

val client = OkHttpClient()

val request = Request.Builder()
    .url("{url}")
    .header("Authorization", "Bearer YOUR_TOKEN")
    .header("Content-Type", "application/json")
'''

        if endpoint.request_body:
            code += f'''    .{method}(
        """{"key": "value"}""".toRequestBody("application/json".toMediaType())
    )
'''
        else:
            code += f'''    .{method}()
'''

        code += '''    .build()

client.newCall(request).execute().use { response ->
    println(response.body?.string())
}'''

        return CodeSample(
            language="kotlin",
            label="Kotlin",
            source=code,
        )

    def _generate_rust(self, endpoint: EndpointDoc) -> CodeSample:
        """Generate Rust code sample."""
        url = f"{self.base_url}{endpoint.path}"
        method = endpoint.method.lower()

        code = f'''use reqwest::header::{{AUTHORIZATION, CONTENT_TYPE}};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {{
    let client = reqwest::Client::new();

    let response = client
        .{method}("{url}")
        .header(AUTHORIZATION, "Bearer YOUR_TOKEN")
        .header(CONTENT_TYPE, "application/json")
'''

        if endpoint.request_body:
            code += '''        .json(&serde_json::json!({"key": "value"}))
'''

        code += '''        .send()
        .await?;

    println!("{}", response.text().await?);
    Ok(())
}'''

        return CodeSample(
            language="rust",
            label="Rust",
            source=code,
        )


class SDKGenerator:
    """
    Generates SDK clients from OpenAPI specifications.

    Features:
    - Multi-language support
    - Type-safe clients
    - Authentication handling
    - Error handling patterns
    """

    def __init__(self, openapi_spec: Dict[str, Any]):
        self.spec = openapi_spec
        self.info = openapi_spec.get("info", {})
        self.paths = openapi_spec.get("paths", {})
        self.components = openapi_spec.get("components", {})

    def generate(self, language: SDKLanguage) -> str:
        """Generate SDK for specified language."""
        generators = {
            SDKLanguage.PYTHON: self._generate_python_sdk,
            SDKLanguage.TYPESCRIPT: self._generate_typescript_sdk,
            SDKLanguage.GO: self._generate_go_sdk,
        }

        generator = generators.get(language)
        if generator:
            return generator()

        return f"// SDK generation for {language.value} not yet implemented"

    def _generate_python_sdk(self) -> str:
        """Generate Python SDK."""
        title = self.info.get("title", "API").replace(" ", "")
        version = self.info.get("version", "1.0.0")

        code = f'''"""
{self.info.get("title", "API")} SDK

Auto-generated Python client for {self.info.get("title", "API")}.
Version: {version}
"""

from typing import Any, Dict, Optional
import requests


class {title}Client:
    """Client for {self.info.get("title", "API")}."""

    def __init__(
        self,
        base_url: str,
        api_key: Optional[str] = None,
        bearer_token: Optional[str] = None,
        timeout: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.bearer_token = bearer_token
        self.timeout = timeout
        self.session = requests.Session()
        self._setup_auth()

    def _setup_auth(self) -> None:
        """Configure authentication headers."""
        if self.api_key:
            self.session.headers["X-API-Key"] = self.api_key
        if self.bearer_token:
            self.session.headers["Authorization"] = f"Bearer {{self.bearer_token}}"

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request."""
        url = f"{{self.base_url}}{{path}}"
        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=data,
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()

'''

        # Generate methods for each endpoint
        for path, methods in self.paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "patch", "delete"]:
                    operation_id = details.get("operationId", f"{method}_{path.replace('/', '_')}")
                    summary = details.get("summary", "")

                    # Clean operation ID to be a valid Python function name
                    func_name = re.sub(r'[^a-zA-Z0-9_]', '_', operation_id).lower()

                    code += f'''    def {func_name}(
        self,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """{summary or f'{method.upper()} {path}'}"""
        return self._request("{method.upper()}", "{path}", params=params, data=data)

'''

        return code

    def _generate_typescript_sdk(self) -> str:
        """Generate TypeScript SDK."""
        title = self.info.get("title", "API").replace(" ", "")

        code = f'''/**
 * {self.info.get("title", "API")} SDK
 *
 * Auto-generated TypeScript client.
 * Version: {self.info.get("version", "1.0.0")}
 */

export interface ClientConfig {{
  baseUrl: string;
  apiKey?: string;
  bearerToken?: string;
  timeout?: number;
}}

export class {title}Client {{
  private baseUrl: string;
  private headers: Record<string, string> = {{}};
  private timeout: number;

  constructor(config: ClientConfig) {{
    this.baseUrl = config.baseUrl.replace(/\\/$/, "");
    this.timeout = config.timeout || 30000;

    if (config.apiKey) {{
      this.headers["X-API-Key"] = config.apiKey;
    }}
    if (config.bearerToken) {{
      this.headers["Authorization"] = `Bearer ${{config.bearerToken}}`;
    }}
  }}

  private async request<T>(
    method: string,
    path: string,
    options?: {{ params?: Record<string, string>; data?: unknown }}
  ): Promise<T> {{
    const url = new URL(`${{this.baseUrl}}${{path}}`);
    if (options?.params) {{
      Object.entries(options.params).forEach(([k, v]) =>
        url.searchParams.set(k, v)
      );
    }}

    const response = await fetch(url.toString(), {{
      method,
      headers: {{
        ...this.headers,
        "Content-Type": "application/json",
      }},
      body: options?.data ? JSON.stringify(options.data) : undefined,
    }});

    if (!response.ok) {{
      throw new Error(`HTTP ${{response.status}}: ${{response.statusText}}`);
    }}

    return response.json();
  }}

'''

        # Generate methods
        for path, methods in self.paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "patch", "delete"]:
                    operation_id = details.get("operationId", f"{method}_{path.replace('/', '_')}")
                    summary = details.get("summary", "")

                    func_name = re.sub(r'[^a-zA-Z0-9_]', '', operation_id)
                    func_name = func_name[0].lower() + func_name[1:] if func_name else func_name

                    code += f'''  /**
   * {summary or f'{method.upper()} {path}'}
   */
  async {func_name}(
    params?: Record<string, string>,
    data?: unknown
  ): Promise<unknown> {{
    return this.request("{method.upper()}", "{path}", {{ params, data }});
  }}

'''

        code += "}\n"
        return code

    def _generate_go_sdk(self) -> str:
        """Generate Go SDK."""
        title = self.info.get("title", "API").replace(" ", "")
        package_name = title.lower()

        code = f'''// Package {package_name} provides a client for {self.info.get("title", "API")}.
//
// Auto-generated Go client.
// Version: {self.info.get("version", "1.0.0")}
package {package_name}

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io/ioutil"
    "net/http"
    "time"
)

// Client is the API client.
type Client struct {{
    BaseURL    string
    APIKey     string
    BearerToken string
    HTTPClient *http.Client
}}

// NewClient creates a new API client.
func NewClient(baseURL string) *Client {{
    return &Client{{
        BaseURL: baseURL,
        HTTPClient: &http.Client{{
            Timeout: 30 * time.Second,
        }},
    }}
}}

// WithAPIKey sets the API key for authentication.
func (c *Client) WithAPIKey(key string) *Client {{
    c.APIKey = key
    return c
}}

// WithBearerToken sets the bearer token for authentication.
func (c *Client) WithBearerToken(token string) *Client {{
    c.BearerToken = token
    return c
}}

func (c *Client) doRequest(method, path string, body interface{{}}) ([]byte, error) {{
    var reqBody *bytes.Buffer
    if body != nil {{
        jsonBody, err := json.Marshal(body)
        if err != nil {{
            return nil, err
        }}
        reqBody = bytes.NewBuffer(jsonBody)
    }}

    req, err := http.NewRequest(method, c.BaseURL+path, reqBody)
    if err != nil {{
        return nil, err
    }}

    req.Header.Set("Content-Type", "application/json")
    if c.APIKey != "" {{
        req.Header.Set("X-API-Key", c.APIKey)
    }}
    if c.BearerToken != "" {{
        req.Header.Set("Authorization", "Bearer "+c.BearerToken)
    }}

    resp, err := c.HTTPClient.Do(req)
    if err != nil {{
        return nil, err
    }}
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {{
        return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
    }}

    return ioutil.ReadAll(resp.Body)
}}

'''

        # Generate methods
        for path, methods in self.paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "patch", "delete"]:
                    operation_id = details.get("operationId", f"{method}_{path.replace('/', '_')}")
                    summary = details.get("summary", f"{method.upper()} {path}")

                    # Convert to Go function name (PascalCase)
                    func_name = "".join(word.capitalize() for word in re.split(r'[^a-zA-Z0-9]', operation_id) if word)

                    code += f'''// {func_name} - {summary}
func (c *Client) {func_name}(body interface{{}}) ([]byte, error) {{
    return c.doRequest("{method.upper()}", "{path}", body)
}}

'''

        return code


class DocumentationExporter:
    """
    Exports API documentation in various formats.

    Formats:
    - OpenAPI JSON/YAML
    - Markdown
    - HTML (static)
    - Postman Collection
    - Insomnia Collection
    """

    def __init__(self, openapi_spec: Dict[str, Any]):
        self.spec = openapi_spec

    def export(self, format: DocumentationFormat) -> str:
        """Export documentation in specified format."""
        exporters = {
            DocumentationFormat.OPENAPI_JSON: self._export_openapi_json,
            DocumentationFormat.OPENAPI_YAML: self._export_openapi_yaml,
            DocumentationFormat.MARKDOWN: self._export_markdown,
            DocumentationFormat.POSTMAN: self._export_postman,
            DocumentationFormat.INSOMNIA: self._export_insomnia,
        }

        exporter = exporters.get(format)
        if exporter:
            return exporter()

        return json.dumps(self.spec, indent=2)

    def _export_openapi_json(self) -> str:
        """Export as OpenAPI JSON."""
        return json.dumps(self.spec, indent=2)

    def _export_openapi_yaml(self) -> str:
        """Export as OpenAPI YAML."""
        try:
            import yaml
            return yaml.dump(self.spec, default_flow_style=False, sort_keys=False)
        except ImportError:
            logger.warning("PyYAML not installed, falling back to JSON")
            return self._export_openapi_json()

    def _export_markdown(self) -> str:
        """Export as Markdown documentation."""
        info = self.spec.get("info", {})
        paths = self.spec.get("paths", {})

        md = f"""# {info.get("title", "API Documentation")}

{info.get("description", "")}

**Version:** {info.get("version", "1.0.0")}

## Endpoints

"""

        for path, methods in paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "patch", "delete"]:
                    md += f"""### {method.upper()} `{path}`

{details.get("summary", "")}

{details.get("description", "")}

"""
                    # Parameters
                    params = details.get("parameters", [])
                    if params:
                        md += "**Parameters:**\n\n"
                        md += "| Name | In | Type | Required | Description |\n"
                        md += "|------|----|----|----------|-------------|\n"
                        for param in params:
                            md += f"| {param.get('name', '')} | {param.get('in', '')} | {param.get('schema', {}).get('type', '')} | {param.get('required', False)} | {param.get('description', '')} |\n"
                        md += "\n"

                    # Responses
                    responses = details.get("responses", {})
                    if responses:
                        md += "**Responses:**\n\n"
                        for code, response in responses.items():
                            md += f"- `{code}`: {response.get('description', '')}\n"
                        md += "\n"

                    md += "---\n\n"

        return md

    def _export_postman(self) -> str:
        """Export as Postman Collection v2.1."""
        info = self.spec.get("info", {})
        paths = self.spec.get("paths", {})
        servers = self.spec.get("servers", [{"url": "https://api.example.com"}])

        collection = {
            "info": {
                "name": info.get("title", "API"),
                "description": info.get("description", ""),
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json",
            },
            "item": [],
            "variable": [
                {
                    "key": "baseUrl",
                    "value": servers[0].get("url", ""),
                }
            ],
        }

        for path, methods in paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "patch", "delete"]:
                    item = {
                        "name": details.get("summary", f"{method.upper()} {path}"),
                        "request": {
                            "method": method.upper(),
                            "header": [
                                {"key": "Content-Type", "value": "application/json"},
                            ],
                            "url": {
                                "raw": "{{baseUrl}}" + path,
                                "host": ["{{baseUrl}}"],
                                "path": [p for p in path.split("/") if p],
                            },
                        },
                    }

                    if details.get("requestBody"):
                        item["request"]["body"] = {
                            "mode": "raw",
                            "raw": "{}",
                        }

                    collection["item"].append(item)

        return json.dumps(collection, indent=2)

    def _export_insomnia(self) -> str:
        """Export as Insomnia Collection v4."""
        info = self.spec.get("info", {})
        paths = self.spec.get("paths", {})
        servers = self.spec.get("servers", [{"url": "https://api.example.com"}])

        resources = []
        base_url = servers[0].get("url", "")

        # Add workspace
        resources.append({
            "_id": "wrk_1",
            "_type": "workspace",
            "name": info.get("title", "API"),
            "description": info.get("description", ""),
        })

        # Add environment
        resources.append({
            "_id": "env_1",
            "_type": "environment",
            "parentId": "wrk_1",
            "name": "Base Environment",
            "data": {"base_url": base_url},
        })

        # Add requests
        req_id = 1
        for path, methods in paths.items():
            for method, details in methods.items():
                if method in ["get", "post", "put", "patch", "delete"]:
                    resources.append({
                        "_id": f"req_{req_id}",
                        "_type": "request",
                        "parentId": "wrk_1",
                        "name": details.get("summary", f"{method.upper()} {path}"),
                        "method": method.upper(),
                        "url": "{{ _.base_url }}" + path,
                        "headers": [
                            {"name": "Content-Type", "value": "application/json"},
                        ],
                    })
                    req_id += 1

        return json.dumps({"_type": "export", "__export_format": 4, "resources": resources}, indent=2)


# Convenience functions

def create_openapi_generator(
    title: str,
    version: str,
    description: str = "",
) -> OpenAPIGenerator:
    """Create an OpenAPI generator with basic info."""
    info = APIInfo(
        title=title,
        version=version,
        description=description,
    )
    return OpenAPIGenerator(info)


def generate_code_samples(
    endpoint: EndpointDoc,
    base_url: str = "https://api.example.com",
) -> List[CodeSample]:
    """Generate code samples for an endpoint in all languages."""
    generator = CodeSampleGenerator(base_url)
    return generator.generate_all(endpoint)


def generate_sdk(
    openapi_spec: Dict[str, Any],
    language: SDKLanguage,
) -> str:
    """Generate SDK from OpenAPI spec."""
    generator = SDKGenerator(openapi_spec)
    return generator.generate(language)


def export_documentation(
    openapi_spec: Dict[str, Any],
    format: DocumentationFormat,
) -> str:
    """Export documentation in specified format."""
    exporter = DocumentationExporter(openapi_spec)
    return exporter.export(format)
