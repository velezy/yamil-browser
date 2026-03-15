"""
Text Processing Utilities for TTS

Provides intelligent text analysis and semantic preprocessing for natural speech synthesis:
- Number-to-words conversion (123 → "one hundred twenty three")
- Currency formatting ($50 → "fifty dollars")
- Date/time formatting (01/15/2024 → "January fifteenth, twenty twenty four")
- Ordinal numbers (1st → "first")
- Abbreviation expansion (Dr. → "Doctor")
- Unit expansion (5km → "five kilometers")
- Acronym handling (NASA vs F.B.I.)
- Markdown stripping and code block handling
"""

import re
import logging
from typing import Optional, Tuple

logger = logging.getLogger(__name__)

# Try to import inflect for number-to-words
try:
    import inflect
    _inflect_engine = inflect.engine()
    INFLECT_AVAILABLE = True
except ImportError:
    _inflect_engine = None
    INFLECT_AVAILABLE = False
    logger.warning("inflect not available. Install with: pip install inflect")

# Try to import markdown-text-clean
try:
    from markdown_text_clean import clean_text as _md_clean
    MARKDOWN_CLEAN_AVAILABLE = True
except ImportError:
    MARKDOWN_CLEAN_AVAILABLE = False
    logger.warning("markdown-text-clean not available. Install with: pip install markdown-text-clean")


# ============================================================================
# SEMANTIC PREPROCESSING - Convert text to natural speech
# ============================================================================

# Common abbreviations and their expansions
ABBREVIATIONS = {
    # Titles
    r'\bDr\.': 'Doctor',
    r'\bMr\.': 'Mister',
    r'\bMrs\.': 'Missus',
    r'\bMs\.': 'Miss',
    r'\bProf\.': 'Professor',
    r'\bSr\.': 'Senior',
    r'\bJr\.': 'Junior',
    r'\bRev\.': 'Reverend',
    r'\bGen\.': 'General',
    r'\bCol\.': 'Colonel',
    r'\bLt\.': 'Lieutenant',
    r'\bSgt\.': 'Sergeant',
    r'\bCapt\.': 'Captain',
    # Places
    r'\bSt\.': 'Street',
    r'\bAve\.': 'Avenue',
    r'\bBlvd\.': 'Boulevard',
    r'\bRd\.': 'Road',
    r'\bDr\.(?=\s+\d)': 'Drive',  # Dr. followed by number (address)
    r'\bLn\.': 'Lane',
    r'\bCt\.': 'Court',
    r'\bPl\.': 'Place',
    r'\bMt\.': 'Mount',
    r'\bFt\.': 'Fort',
    # Common
    r'\betc\.': 'etcetera',
    r'\be\.g\.': 'for example',
    r'\bi\.e\.': 'that is',
    r'\bvs\.': 'versus',
    r'\bw/': 'with',
    r'\bw/o': 'without',
    r'\bapprox\.': 'approximately',
    r'\best\.': 'estimated',
    r'\bmin\.': 'minimum',
    r'\bmax\.': 'maximum',
    r'\bavg\.': 'average',
    r'\bno\.': 'number',
    r'\bvol\.': 'volume',
    r'\bpp\.': 'pages',
    r'\bp\.': 'page',
    r'\bfig\.': 'figure',
    r'\bch\.': 'chapter',
    r'\bsec\.': 'section',
    # Business
    r'\bInc\.': 'Incorporated',
    r'\bLtd\.': 'Limited',
    r'\bCo\.': 'Company',
    r'\bCorp\.': 'Corporation',
    r'\bLLC': 'L L C',
    # Time
    r'\ba\.m\.': 'A M',
    r'\bp\.m\.': 'P M',
    r'\bAM\b': 'A M',
    r'\bPM\b': 'P M',
}

# Units and their spoken forms
UNITS = {
    # Length
    r'(\d+(?:\.\d+)?)\s*km\b': r'\1 kilometers',
    r'(\d+(?:\.\d+)?)\s*m\b': r'\1 meters',
    r'(\d+(?:\.\d+)?)\s*cm\b': r'\1 centimeters',
    r'(\d+(?:\.\d+)?)\s*mm\b': r'\1 millimeters',
    r'(\d+(?:\.\d+)?)\s*mi\b': r'\1 miles',
    r'(\d+(?:\.\d+)?)\s*ft\b': r'\1 feet',
    r'(\d+(?:\.\d+)?)\s*in\b': r'\1 inches',
    r'(\d+(?:\.\d+)?)\s*yd\b': r'\1 yards',
    # Weight
    r'(\d+(?:\.\d+)?)\s*kg\b': r'\1 kilograms',
    r'(\d+(?:\.\d+)?)\s*g\b': r'\1 grams',
    r'(\d+(?:\.\d+)?)\s*mg\b': r'\1 milligrams',
    r'(\d+(?:\.\d+)?)\s*lb\b': r'\1 pounds',
    r'(\d+(?:\.\d+)?)\s*lbs\b': r'\1 pounds',
    r'(\d+(?:\.\d+)?)\s*oz\b': r'\1 ounces',
    # Volume
    r'(\d+(?:\.\d+)?)\s*L\b': r'\1 liters',
    r'(\d+(?:\.\d+)?)\s*ml\b': r'\1 milliliters',
    r'(\d+(?:\.\d+)?)\s*mL\b': r'\1 milliliters',
    r'(\d+(?:\.\d+)?)\s*gal\b': r'\1 gallons',
    # Temperature
    r'(\d+(?:\.\d+)?)\s*°C\b': r'\1 degrees Celsius',
    r'(\d+(?:\.\d+)?)\s*°F\b': r'\1 degrees Fahrenheit',
    r'(\d+(?:\.\d+)?)\s*K\b': r'\1 Kelvin',
    # Speed
    r'(\d+(?:\.\d+)?)\s*mph\b': r'\1 miles per hour',
    r'(\d+(?:\.\d+)?)\s*km/h\b': r'\1 kilometers per hour',
    r'(\d+(?:\.\d+)?)\s*m/s\b': r'\1 meters per second',
    # Data
    r'(\d+(?:\.\d+)?)\s*TB\b': r'\1 terabytes',
    r'(\d+(?:\.\d+)?)\s*GB\b': r'\1 gigabytes',
    r'(\d+(?:\.\d+)?)\s*MB\b': r'\1 megabytes',
    r'(\d+(?:\.\d+)?)\s*KB\b': r'\1 kilobytes',
    r'(\d+(?:\.\d+)?)\s*B\b': r'\1 bytes',
    # Time
    r'(\d+(?:\.\d+)?)\s*hrs?\b': r'\1 hours',
    r'(\d+(?:\.\d+)?)\s*mins?\b': r'\1 minutes',
    r'(\d+(?:\.\d+)?)\s*secs?\b': r'\1 seconds',
    r'(\d+(?:\.\d+)?)\s*ms\b': r'\1 milliseconds',
    # Frequency
    r'(\d+(?:\.\d+)?)\s*GHz\b': r'\1 gigahertz',
    r'(\d+(?:\.\d+)?)\s*MHz\b': r'\1 megahertz',
    r'(\d+(?:\.\d+)?)\s*kHz\b': r'\1 kilohertz',
    r'(\d+(?:\.\d+)?)\s*Hz\b': r'\1 hertz',
}

# Acronyms that should be spelled out letter by letter
SPELL_OUT_ACRONYMS = {
    'FBI', 'CIA', 'NSA', 'IRS', 'DMV', 'CEO', 'CFO', 'CTO', 'COO',
    'USA', 'UK', 'EU', 'UN', 'WHO', 'HIV', 'AIDS', 'DNA', 'RNA',
    'API', 'URL', 'HTML', 'CSS', 'SQL', 'PHP', 'XML', 'JSON',
    'CPU', 'GPU', 'RAM', 'ROM', 'SSD', 'HDD', 'USB', 'HDMI',
    'PDF', 'JPG', 'PNG', 'GIF', 'MP3', 'MP4', 'AVI',
    'ATM', 'PIN', 'VIP', 'DIY', 'FAQ', 'FYI', 'ASAP',
    'NYC', 'LA', 'SF', 'DC', 'TV', 'PC', 'AC', 'DC',
}

# Acronyms that should be spoken as words
WORD_ACRONYMS = {
    'NASA', 'NATO', 'RADAR', 'LASER', 'SCUBA', 'AIDS', 'UNICEF',
    'OPEC', 'IKEA', 'CAPTCHA', 'GIF', 'JPEG', 'RAM', 'ROM',
}

# Month names for date conversion
MONTHS = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}


def _number_to_words(num: float) -> str:
    """Convert a number to spoken words."""
    if not INFLECT_AVAILABLE:
        return str(num)

    try:
        if num == int(num):
            return _inflect_engine.number_to_words(int(num))
        else:
            # Handle decimals
            int_part = int(num)
            dec_part = str(num).split('.')[1]
            int_words = _inflect_engine.number_to_words(int_part)
            dec_words = ' '.join(_inflect_engine.number_to_words(int(d)) for d in dec_part)
            return f"{int_words} point {dec_words}"
    except Exception:
        return str(num)


def _ordinal_to_words(num: int) -> str:
    """Convert ordinal number to words (1st → first)."""
    if not INFLECT_AVAILABLE:
        return f"{num}th"

    try:
        return _inflect_engine.ordinal(_inflect_engine.number_to_words(num))
    except Exception:
        return f"{num}th"


def _expand_abbreviations(text: str) -> str:
    """Expand common abbreviations to full words."""
    for pattern, replacement in ABBREVIATIONS.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def _expand_units(text: str) -> str:
    """Expand unit abbreviations to full words."""
    for pattern, replacement in UNITS.items():
        text = re.sub(pattern, replacement, text, flags=re.IGNORECASE)
    return text


def _convert_currency(text: str) -> str:
    """Convert currency to spoken form."""

    # First handle currency with magnitude (million, billion, trillion)
    def replace_currency_with_magnitude(match):
        symbol = match.group(1)
        amount = match.group(2).replace(',', '')
        magnitude = match.group(3).lower()

        currency_name = {
            '$': 'dollars',
            '£': 'pounds',
            '€': 'euros',
            '¥': 'yen',
        }.get(symbol, 'dollars')

        try:
            num = float(amount)
            if INFLECT_AVAILABLE:
                num_word = _number_to_words(num)
            else:
                num_word = str(num)
            return f"{num_word} {magnitude} {currency_name}"
        except ValueError:
            return match.group(0)

    # Match $24.96 million, $5 billion, etc.
    text = re.sub(
        r'([$£€¥])\s*(\d+(?:\.\d+)?)\s*(million|billion|trillion|thousand)',
        replace_currency_with_magnitude,
        text,
        flags=re.IGNORECASE
    )

    # Then handle regular currency (without magnitude)
    def replace_currency(match):
        symbol = match.group(1)
        amount = match.group(2).replace(',', '')

        try:
            num = float(amount)
            dollars = int(num)
            cents = int(round((num - dollars) * 100))

            currency_name = {
                '$': ('dollar', 'dollars', 'cent', 'cents'),
                '£': ('pound', 'pounds', 'penny', 'pence'),
                '€': ('euro', 'euros', 'cent', 'cents'),
                '¥': ('yen', 'yen', '', ''),
            }.get(symbol, ('dollar', 'dollars', 'cent', 'cents'))

            if not INFLECT_AVAILABLE:
                if cents > 0:
                    return f"{dollars} {currency_name[1]} and {cents} {currency_name[3]}"
                return f"{dollars} {currency_name[1]}"

            dollar_word = _inflect_engine.number_to_words(dollars)
            dollar_unit = currency_name[0] if dollars == 1 else currency_name[1]

            if cents > 0 and currency_name[2]:
                cent_word = _inflect_engine.number_to_words(cents)
                cent_unit = currency_name[2] if cents == 1 else currency_name[3]
                return f"{dollar_word} {dollar_unit} and {cent_word} {cent_unit}"

            return f"{dollar_word} {dollar_unit}"
        except ValueError:
            return match.group(0)

    # Match currency patterns: $123.45, £50, €100, etc. (but not followed by magnitude words)
    text = re.sub(
        r'([$£€¥])\s*(\d{1,3}(?:,\d{3})*(?:\.\d{1,2})?)(?!\s*(?:million|billion|trillion|thousand))',
        replace_currency,
        text,
        flags=re.IGNORECASE
    )
    return text


def _convert_dates(text: str) -> str:
    """Convert dates to spoken form."""
    def replace_date(match):
        try:
            # Try MM/DD/YYYY or MM-DD-YYYY
            month = int(match.group(1))
            day = int(match.group(2))
            year = int(match.group(3))

            if 1 <= month <= 12 and 1 <= day <= 31:
                month_name = MONTHS[month]
                day_ordinal = _ordinal_to_words(day) if INFLECT_AVAILABLE else f"{day}th"

                # Convert year to words
                if INFLECT_AVAILABLE:
                    if 2000 <= year <= 2099:
                        year_words = _inflect_engine.number_to_words(year)
                    elif 1900 <= year <= 1999:
                        first = year // 100
                        second = year % 100
                        if second == 0:
                            year_words = f"{_inflect_engine.number_to_words(first)} hundred"
                        else:
                            year_words = f"{_inflect_engine.number_to_words(first)} {_inflect_engine.number_to_words(second)}"
                    else:
                        year_words = _inflect_engine.number_to_words(year)
                else:
                    year_words = str(year)

                return f"{month_name} {day_ordinal}, {year_words}"
        except (ValueError, KeyError):
            pass
        return match.group(0)

    # Match date patterns
    text = re.sub(r'(\d{1,2})[/\-](\d{1,2})[/\-](\d{4})', replace_date, text)
    return text


def _convert_times(text: str) -> str:
    """Convert times to spoken form."""
    def replace_time(match):
        try:
            hour = int(match.group(1))
            minute = int(match.group(2))
            period = match.group(3) if match.lastindex >= 3 else ''

            if INFLECT_AVAILABLE:
                hour_word = _inflect_engine.number_to_words(hour)
                if minute == 0:
                    minute_word = "o'clock"
                elif minute < 10:
                    minute_word = f"oh {_inflect_engine.number_to_words(minute)}"
                else:
                    minute_word = _inflect_engine.number_to_words(minute)

                result = f"{hour_word} {minute_word}"
                if period:
                    result += f" {period.upper().replace('.', ' ').strip()}"
                return result
            else:
                return match.group(0)
        except ValueError:
            return match.group(0)

    # Match time patterns: 3:45, 3:45pm, 3:45 PM
    text = re.sub(r'(\d{1,2}):(\d{2})\s*([apAP]\.?[mM]\.?)?', replace_time, text)
    return text


def _convert_ordinals(text: str) -> str:
    """Convert ordinal numbers to words (1st → first)."""
    def replace_ordinal(match):
        num = int(match.group(1))
        return _ordinal_to_words(num)

    # Match ordinal patterns: 1st, 2nd, 3rd, 4th, etc.
    text = re.sub(r'\b(\d+)(?:st|nd|rd|th)\b', replace_ordinal, text)
    return text


def _convert_numbers_to_words(text: str) -> str:
    """Convert standalone numbers to words."""
    def replace_number(match):
        num_str = match.group(0).replace(',', '')
        try:
            num = float(num_str)
            # Only convert reasonable numbers (not too large)
            if abs(num) <= 999999999:
                return _number_to_words(num)
        except ValueError:
            pass
        return match.group(0)

    # Match standalone numbers (not part of dates, times, or units)
    # This is applied after other conversions
    text = re.sub(r'\b\d{1,3}(?:,\d{3})*(?:\.\d+)?\b', replace_number, text)
    return text


def _handle_acronyms(text: str) -> str:
    """Handle acronyms - spell out or pronounce as words."""
    def replace_acronym(match):
        acronym = match.group(0)
        upper = acronym.upper()

        # Check if it should be spelled out
        if upper in SPELL_OUT_ACRONYMS:
            return ' '.join(upper)

        # Check if it's a word acronym (leave as-is for TTS to handle)
        if upper in WORD_ACRONYMS:
            return acronym

        # For unknown all-caps 2-4 letter words, spell them out
        if len(acronym) <= 4 and acronym.isupper():
            return ' '.join(acronym)

        return acronym

    # Match potential acronyms (2-6 uppercase letters)
    text = re.sub(r'\b[A-Z]{2,6}\b', replace_acronym, text)
    return text


def _convert_math_expressions(text: str) -> str:
    """Convert simple math expressions to spoken form."""
    replacements = [
        (r'(\d+)\s*\+\s*(\d+)\s*=\s*(\d+)', r'\1 plus \2 equals \3'),
        (r'(\d+)\s*-\s*(\d+)\s*=\s*(\d+)', r'\1 minus \2 equals \3'),
        (r'(\d+)\s*[×x\*]\s*(\d+)\s*=\s*(\d+)', r'\1 times \2 equals \3'),
        (r'(\d+)\s*[÷/]\s*(\d+)\s*=\s*(\d+)', r'\1 divided by \2 equals \3'),
        (r'(\d+)\s*\^(\d+)', r'\1 to the power of \2'),
        (r'√(\d+)', r'square root of \1'),
        (r'(\d+)%', r'\1 percent'),
    ]

    for pattern, replacement in replacements:
        text = re.sub(pattern, replacement, text)

    return text


def _convert_latex_to_speech(text: str) -> str:
    """Convert LaTeX math expressions to spoken form for TTS."""

    # Remove display math delimiters ($$...$$ or \[...\])
    text = re.sub(r'\$\$(.*?)\$\$', r' \1 ', text, flags=re.DOTALL)
    text = re.sub(r'\\\[(.*?)\\\]', r' \1 ', text, flags=re.DOTALL)

    # Remove inline math delimiters ($...$)
    text = re.sub(r'\$([^$]+)\$', r' \1 ', text)

    # Greek letters
    greek_letters = {
        r'\\alpha': 'alpha', r'\\beta': 'beta', r'\\gamma': 'gamma',
        r'\\delta': 'delta', r'\\epsilon': 'epsilon', r'\\zeta': 'zeta',
        r'\\eta': 'eta', r'\\theta': 'theta', r'\\iota': 'iota',
        r'\\kappa': 'kappa', r'\\lambda': 'lambda', r'\\mu': 'mu',
        r'\\nu': 'nu', r'\\xi': 'xi', r'\\pi': 'pi', r'\\rho': 'rho',
        r'\\sigma': 'sigma', r'\\tau': 'tau', r'\\upsilon': 'upsilon',
        r'\\phi': 'phi', r'\\chi': 'chi', r'\\psi': 'psi', r'\\omega': 'omega',
        r'\\Alpha': 'Alpha', r'\\Beta': 'Beta', r'\\Gamma': 'Gamma',
        r'\\Delta': 'Delta', r'\\Theta': 'Theta', r'\\Pi': 'Pi',
        r'\\Sigma': 'Sigma', r'\\Phi': 'Phi', r'\\Omega': 'Omega',
    }
    for latex, spoken in greek_letters.items():
        text = re.sub(latex + r'\b', spoken, text)

    # Common math functions and operators
    math_functions = {
        r'\\sin': 'sine of', r'\\cos': 'cosine of', r'\\tan': 'tangent of',
        r'\\arcsin': 'arc sine of', r'\\arccos': 'arc cosine of', r'\\arctan': 'arc tangent of',
        r'\\log': 'log of', r'\\ln': 'natural log of', r'\\exp': 'e to the',
        r'\\lim': 'limit of', r'\\sum': 'sum of', r'\\prod': 'product of',
        r'\\int': 'integral of', r'\\infty': 'infinity',
        r'\\pm': 'plus or minus', r'\\mp': 'minus or plus',
        r'\\times': 'times', r'\\div': 'divided by', r'\\cdot': 'times',
        r'\\leq': 'less than or equal to', r'\\geq': 'greater than or equal to',
        r'\\neq': 'not equal to', r'\\approx': 'approximately equal to',
        r'\\equiv': 'equivalent to', r'\\rightarrow': 'goes to',
        r'\\Rightarrow': 'implies', r'\\therefore': 'therefore',
    }
    for latex, spoken in math_functions.items():
        text = re.sub(latex + r'(?:\s|{|$)', ' ' + spoken + ' ', text)

    # Fractions: \frac{a}{b} -> "a over b"
    frac_pattern = r'\\frac\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}'
    while re.search(frac_pattern, text):
        text = re.sub(frac_pattern, r' \1 over \2 ', text)

    # Square root: \sqrt{x} -> "square root of x"
    sqrt_pattern = r'\\sqrt\s*\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}'
    while re.search(sqrt_pattern, text):
        text = re.sub(sqrt_pattern, r' square root of \1 ', text)

    # nth root: \sqrt[n]{x} -> "nth root of x"
    text = re.sub(r'\\sqrt\s*\[(\d+)\]\s*\{([^{}]*)\}', r' \1th root of \2 ', text)

    # Superscripts: x^{2} or x^2 -> "x to the power of 2" or "x squared/cubed"
    text = re.sub(r'\^{2}|\^2', ' squared', text)
    text = re.sub(r'\^{3}|\^3', ' cubed', text)
    text = re.sub(r'\^\{([^{}]+)\}', r' to the power of \1', text)
    text = re.sub(r'\^(\d+)', r' to the power of \1', text)
    text = re.sub(r'\^([a-zA-Z])', r' to the power of \1', text)

    # Subscripts: x_{1} or x_1 -> "x sub 1"
    text = re.sub(r'_\{([^{}]+)\}', r' sub \1', text)
    text = re.sub(r'_(\d+)', r' sub \1', text)
    text = re.sub(r'_([a-zA-Z])', r' sub \1', text)

    # Remove remaining LaTeX commands like \text{...}, \mathrm{...}, etc.
    text = re.sub(r'\\(?:text|mathrm|mathbf|mathit|mathbb|mathcal)\s*\{([^{}]*)\}', r'\1', text)

    # Remove \left and \right
    text = re.sub(r'\\(?:left|right|big|Big|bigg|Bigg)', '', text)

    # Remove remaining backslash commands that weren't handled
    text = re.sub(r'\\[a-zA-Z]+', ' ', text)

    # Clean up curly braces
    text = re.sub(r'\{|\}', '', text)

    # Clean up extra whitespace
    text = re.sub(r'\s+', ' ', text)

    return text


def semantic_preprocess(text: str) -> str:
    """
    Apply full semantic preprocessing to convert text to natural speech.

    This is the main entry point for semantic preprocessing.
    Order matters - some conversions depend on others.

    Args:
        text: Raw text to preprocess

    Returns:
        Text optimized for natural TTS speech
    """
    if not text:
        return ""

    # Step 1: Expand abbreviations first (before number processing)
    text = _expand_abbreviations(text)

    # Step 2: Convert currency (before general number conversion)
    text = _convert_currency(text)

    # Step 3: Convert dates
    text = _convert_dates(text)

    # Step 4: Convert times
    text = _convert_times(text)

    # Step 5: Expand units (before number conversion)
    text = _expand_units(text)

    # Step 6: Convert ordinals (1st → first)
    text = _convert_ordinals(text)

    # Step 7: Convert math expressions
    text = _convert_math_expressions(text)

    # Step 8: Handle acronyms
    text = _handle_acronyms(text)

    # Step 9: Convert remaining numbers to words (last, after all patterns extracted)
    text = _convert_numbers_to_words(text)

    return text


def _remove_system_instructions(text: str) -> str:
    """Remove system instructions, meta-text, AI response markers, and role identifiers."""

    # Remove "Improved Response" and similar meta-markers
    patterns_to_remove = [
        # Response markers
        r'\*\*Improved Response\*\*\s*',
        r'\*\*Response\*\*\s*',
        r'\*\*Answer\*\*\s*',
        r'\*\*Final Answer\*\*\s*',

        # Meta commentary
        r'I will recalculate[^.]*\.\s*',
        r'I have addressed your feedback[^.]*\.\s*',
        r'Let me (explain|break down|show you)[^.]*\.\s*',
        r'Here\'s (the|my|a) (improved|updated|revised)[^.]*:\s*',

        # LLM instruction formats
        r'\[INST\][\s\S]*?\[/INST\]\s*',  # Llama instruction tags
        r'<\|im_start\|>[\s\S]*?<\|im_end\|>\s*',  # ChatML markers
        r'<\|assistant\|>\s*',  # Llama role markers
        r'<\|user\|>\s*',
        r'<\|system\|>[\s\S]*?(?=<\||$)',  # System until next marker or end
        r'<system>[\s\S]*?</system>\s*',  # System tags
        r'<thinking>[\s\S]*?</thinking>\s*',  # Thinking tags
        r'<<SYS>>[\s\S]*?<</SYS>>\s*',  # Llama 2 system format
        r'\[SYS\][\s\S]*?\[/SYS\]\s*',  # Alternative system format

        # Role markers at start of lines or text
        r'^(superadmin|admin|user|assistant|system|bot|ai|human):\s*',  # Role prefixes
        r'\n(superadmin|admin|user|assistant|system|bot|ai|human):\s*',  # Role prefixes after newline
        r'\*\*(superadmin|admin|user|assistant|system|bot|ai|human)\*\*:\s*',  # Bold role prefixes

        # System prompt content markers
        r'You are DriveSentinel[^.]*\.\s*',  # Our system prompt
        r'You are an? (AI|assistant|chatbot|helpful)[^.]*\.\s*',  # Generic AI intros
        r'As an AI (assistant|language model)[^.]*\.\s*',

        # Notes and annotations
        r'\*\*Note:\*\*[^*]*',
        r'\*Note:[^*]*\*',
        r'\[Note:[^\]]*\]',

        # Debug/meta information
        r'\[DEBUG\][^\]]*',
        r'\[INFO\][^\]]*',
        r'Context:\s*\{[^}]*\}\s*',  # JSON context blocks
        r'User role:\s*\w+\s*',  # User role mentions
        r'Current user:\s*[^\n]*\n?',  # Current user mentions

        # Placeholder patterns
        r'\[\s*your.*?\s*\]',  # [your name], [your response], etc.
        r'\{\s*placeholder\s*\}',
    ]

    for pattern in patterns_to_remove:
        text = re.sub(pattern, '', text, flags=re.IGNORECASE | re.MULTILINE)

    return text


def analyze_and_convert_for_speech(text: str) -> str:
    """
    Intelligently analyze content and convert to natural speech.

    This function:
    1. Detects content types (code execution, charts, diagrams, etc.)
    2. Extracts meaningful information from technical content
    3. Applies semantic preprocessing (numbers, dates, currency, units, etc.)
    4. Produces natural, human-like speech output

    Args:
        text: Raw text with markdown, code blocks, etc.

    Returns:
        Natural speech-ready text
    """
    if not text:
        return ""

    # Process the text in sections
    result = text

    # Step 0: Remove system instructions and meta-text
    result = _remove_system_instructions(result)

    # Step 1: Handle code execution results - extract the meaningful output
    result = _convert_code_execution_to_speech(result)

    # Step 2: Handle charts and diagrams
    result = _convert_charts_to_speech(result)

    # Step 3: Handle mermaid diagrams
    result = _convert_mermaid_to_speech(result)

    # Step 4: Convert LaTeX math to spoken form (BEFORE markdown stripping)
    result = _convert_latex_to_speech(result)

    # Step 5: Strip remaining markdown formatting
    result = _strip_markdown_for_speech(result)

    # Step 6: Apply semantic preprocessing (numbers, dates, currency, units, acronyms)
    result = semantic_preprocess(result)

    # Step 7: Final cleanup for TTS
    result = _tts_cleanup(result)

    return result


def _convert_code_execution_to_speech(text: str) -> str:
    """Convert code execution blocks to natural speech."""

    # Pattern: **Code Executed:**...code...```\n\n**Output:**\n```\nresult\n```
    # We want to extract the result and describe it naturally

    # Find and replace code execution patterns
    def replace_code_block(match):
        full_match = match.group(0)

        # Try to extract the output value
        output_match = re.search(r'\*\*Output:\*\*\s*```[^\n]*\n([\s\S]*?)```', full_match)
        if output_match:
            output = output_match.group(1).strip()

            # Check if it's a numeric result
            try:
                # Handle both int and float
                num_value = float(output.replace(',', ''))
                return f" The result is {_format_number_for_speech(num_value)}."
            except ValueError:
                pass

            # If it's short text output, read it
            if len(output) < 100 and not re.search(r'[{}\[\]<>]', output):
                return f" The output is: {output}."

            # Otherwise, just mention there was output
            return " The code was executed successfully."

        return " The code was executed."

    # Match code execution pattern
    pattern = r'\*\*Code Executed:\*\*[\s\S]*?```[\s\S]*?```(?:\s*\*Execution time:[^*]*\*)?(?:\s*\*\*Output:\*\*\s*```[\s\S]*?```)?'
    text = re.sub(pattern, replace_code_block, text)

    # Remove standalone code blocks that aren't part of execution
    text = re.sub(r'```[a-z]*\n[\s\S]*?```', ' ', text)

    # Remove inline code
    text = re.sub(r'`[^`]+`', '', text)

    return text


def _convert_charts_to_speech(text: str) -> str:
    """Convert chart references to natural speech."""

    # Detect chart-related content
    if re.search(r'(?:bar chart|pie chart|line chart|chart showing|graph showing)', text, re.IGNORECASE):
        # Keep the description but remove technical chart config
        pass

    # Remove chartjs or visualization code blocks
    text = re.sub(r'```(?:chartjs|javascript|json)\s*\{[\s\S]*?\}```',
                  ' I\'ve created a visual chart for you. ', text)

    return text


def _convert_mermaid_to_speech(text: str) -> str:
    """Convert mermaid diagrams to natural speech description."""

    # Pattern for mermaid code blocks
    def replace_mermaid(match):
        content = match.group(1)

        # Detect diagram type
        if 'flowchart' in content.lower() or 'graph' in content.lower():
            return " I've created a flowchart diagram for you showing the process flow. "
        elif 'sequenceDiagram' in content:
            return " I've created a sequence diagram showing the interactions. "
        elif 'classDiagram' in content:
            return " I've created a class diagram showing the structure. "
        elif 'pie' in content.lower():
            return " I've created a pie chart visualization. "
        elif 'gantt' in content.lower():
            return " I've created a Gantt chart showing the timeline. "
        else:
            return " I've created a diagram for you. "

    text = re.sub(r'```mermaid\s*([\s\S]*?)```', replace_mermaid, text)

    return text


def _strip_markdown_for_speech(text: str) -> str:
    """Strip markdown formatting while preserving readable content."""

    # Remove images (before links to avoid conflict)
    text = re.sub(r'!\[([^\]]*)\]\([^)]+\)', r'\1', text)

    # Remove links but keep text
    text = re.sub(r'\[([^\]]+)\]\([^)]+\)', r'\1', text)

    # Remove headers (# ## ### etc.) - keep the text
    text = re.sub(r'^#{1,6}\s+', '', text, flags=re.MULTILINE)

    # Remove bold/italic markers but keep text (order matters - do bold first)
    text = re.sub(r'\*\*\*([^*]+)\*\*\*', r'\1', text)  # bold+italic
    text = re.sub(r'___([^_]+)___', r'\1', text)        # bold+italic
    text = re.sub(r'\*\*([^*]+)\*\*', r'\1', text)      # bold
    text = re.sub(r'__([^_]+)__', r'\1', text)          # bold
    text = re.sub(r'\*([^*]+)\*', r'\1', text)          # italic
    text = re.sub(r'_([^_]+)_', r'\1', text)            # italic

    # Remove strikethrough
    text = re.sub(r'~~([^~]+)~~', r'\1', text)

    # Remove blockquote markers
    text = re.sub(r'^>\s*', '', text, flags=re.MULTILINE)

    # Remove horizontal rules
    text = re.sub(r'^[-*_]{3,}\s*$', '', text, flags=re.MULTILINE)

    # Convert list markers to spoken transitions
    text = re.sub(r'^[\s]*[-*+]\s+', 'Next, ', text, flags=re.MULTILINE)
    text = re.sub(r'^[\s]*(\d+)\.\s+', r'Number \1, ', text, flags=re.MULTILINE)

    return text


def _format_number_for_speech(num: float) -> str:
    """Format a number for natural speech."""

    # Handle integers vs floats
    if num == int(num):
        num = int(num)

    # Format with commas for readability
    if isinstance(num, int):
        return f"{num:,}"
    else:
        # Round to reasonable precision
        if abs(num) >= 1:
            return f"{num:,.2f}"
        else:
            return f"{num:.6g}"


def _make_numbers_readable(text: str) -> str:
    """Convert numbers in text to more readable format."""

    def format_match(match):
        num_str = match.group(0)
        try:
            num = float(num_str.replace(',', ''))
            return _format_number_for_speech(num)
        except ValueError:
            return num_str

    # Find standalone numbers (not part of words)
    text = re.sub(r'\b\d+(?:\.\d+)?\b', format_match, text)

    return text


def _tts_cleanup(text: str) -> str:
    """Final cleanup for natural TTS output."""

    # IMPORTANT: Convert escaped newlines to spaces first (prevents TTS reading "nn")
    text = text.replace('\\n\\n', ' ')
    text = text.replace('\\n', ' ')
    text = text.replace('\n\n', ' ')
    text = text.replace('\n', ' ')

    # Remove JSON-like content
    text = re.sub(r'\{[^}]*\}', '', text)
    text = re.sub(r'\[[^\]]*\]', '', text)

    # Remove file paths
    text = re.sub(r'[/\\][\w./\\-]+\.\w+', '', text)

    # Remove variable-like names (snake_case)
    text = re.sub(r'\b\w+_\w+\b', '', text)

    # Replace common symbols with speakable versions
    replacements = {
        '&': ' and ',
        '@': ' at ',
        '%': ' percent ',
        '...': ', ',
        '->': ' to ',
        '=>': ' to ',
        '<=': ' less than or equal to ',
        '>=': ' greater than or equal to ',
        '!=': ' not equal to ',
        '==': ' equals ',
        '&&': ' and ',
        '||': ' or ',
        '---': ' ',
        '***': ' ',
        '  ': ' ',
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # Remove URLs
    text = re.sub(r'https?://\S+', '', text)

    # Remove email addresses
    text = re.sub(r'\S+@\S+\.\S+', '', text)

    # Remove numbers in parentheses like (123)
    text = re.sub(r'\(\d+\)', '', text)

    # Remove standalone special characters
    text = re.sub(r'[#$^*<>|\\`~]', '', text)

    # Normalize whitespace
    text = re.sub(r'\s+', ' ', text)

    # Remove leading/trailing whitespace
    text = text.strip()

    # Clean up punctuation spacing
    text = re.sub(r'\s+([.,!?;:])', r'\1', text)
    text = re.sub(r'([.,!?;:])\s*([.,!?;:])', r'\1', text)  # Remove duplicate punctuation

    return text


# Legacy compatibility functions

def strip_markdown(text: str) -> str:
    """Remove markdown formatting from text for natural TTS."""
    return analyze_and_convert_for_speech(text)


def clean_for_tts(text: str, strip_md: bool = True) -> str:
    """
    Prepare text for text-to-speech synthesis.

    Args:
        text: Input text (potentially with markdown)
        strip_md: Whether to strip markdown formatting

    Returns:
        Clean text optimized for TTS
    """
    if not text:
        return ""

    if strip_md:
        return analyze_and_convert_for_speech(text)
    else:
        return _tts_cleanup(text)


def split_sentences_for_streaming(text: str) -> list[str]:
    """
    Split text into sentences for streaming TTS.

    Intelligently splits on sentence boundaries while handling
    common abbreviations and edge cases.

    Args:
        text: Text to split

    Returns:
        List of sentences
    """
    if not text:
        return []

    # Common abbreviations that shouldn't end sentences
    abbreviations = r'(?:Mr|Mrs|Ms|Dr|Prof|Sr|Jr|vs|etc|e\.g|i\.e|Inc|Ltd|Co)'

    # Split on sentence-ending punctuation, but not after abbreviations
    pattern = rf'(?<!{abbreviations})([.!?])\s+(?=[A-Z])'

    # Split and keep the punctuation with the sentence
    parts = re.split(pattern, text)

    # Rejoin punctuation with sentences
    sentences = []
    i = 0
    while i < len(parts):
        if i + 1 < len(parts) and parts[i + 1] in '.!?':
            sentences.append(parts[i] + parts[i + 1])
            i += 2
        else:
            if parts[i].strip():
                sentences.append(parts[i])
            i += 1

    return sentences


def is_markdown_clean_available() -> bool:
    """Check if markdown-text-clean library is available"""
    return MARKDOWN_CLEAN_AVAILABLE
