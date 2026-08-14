use anyhow::{Result, bail};
use serde::{Deserialize, Serialize};

const LEFT_SHIFT: u8 = 0x02;
const KEY_DELAY_MS: u16 = 12;
const MAX_TEXT_CHARS: usize = 4096;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct MacroStep {
    pub modifier: u8,
    pub keys: [u8; 6],
    pub delay_ms: u16,
}

impl MacroStep {
    fn key(modifier: u8, usage: u8) -> Self {
        Self {
            modifier,
            keys: [usage, 0, 0, 0, 0, 0],
            delay_ms: KEY_DELAY_MS,
        }
    }

    fn release() -> Self {
        Self {
            modifier: 0,
            keys: [0; 6],
            delay_ms: KEY_DELAY_MS,
        }
    }
}

pub fn text_to_macro(text: &str) -> Result<Vec<MacroStep>> {
    if text.chars().count() > MAX_TEXT_CHARS {
        bail!("text exceeds {MAX_TEXT_CHARS} character limit");
    }

    let mut steps = Vec::with_capacity(text.len().saturating_mul(2));
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        if character == '\r' && characters.peek() == Some(&'\n') {
            characters.next();
        }
        let (modifier, usage) = us_ascii_usage(character).ok_or_else(|| {
            anyhow::anyhow!("character is unavailable in US keyboard layout: {character:?}")
        })?;
        steps.push(MacroStep::key(modifier, usage));
        steps.push(MacroStep::release());
    }
    Ok(steps)
}

pub fn us_ascii_usage(character: char) -> Option<(u8, u8)> {
    let value = match character {
        'a'..='z' => (0, 0x04 + (character as u8 - b'a')),
        'A'..='Z' => (LEFT_SHIFT, 0x04 + (character as u8 - b'A')),
        '1'..='9' => (0, 0x1e + (character as u8 - b'1')),
        '0' => (0, 0x27),
        '\n' | '\r' => (0, 0x28),
        '\t' => (0, 0x2b),
        ' ' => (0, 0x2c),
        '-' => (0, 0x2d),
        '_' => (LEFT_SHIFT, 0x2d),
        '=' => (0, 0x2e),
        '+' => (LEFT_SHIFT, 0x2e),
        '[' => (0, 0x2f),
        '{' => (LEFT_SHIFT, 0x2f),
        ']' => (0, 0x30),
        '}' => (LEFT_SHIFT, 0x30),
        '\\' => (0, 0x31),
        '|' => (LEFT_SHIFT, 0x31),
        ';' => (0, 0x33),
        ':' => (LEFT_SHIFT, 0x33),
        '\'' => (0, 0x34),
        '"' => (LEFT_SHIFT, 0x34),
        '`' => (0, 0x35),
        '~' => (LEFT_SHIFT, 0x35),
        ',' => (0, 0x36),
        '<' => (LEFT_SHIFT, 0x36),
        '.' => (0, 0x37),
        '>' => (LEFT_SHIFT, 0x37),
        '/' => (0, 0x38),
        '?' => (LEFT_SHIFT, 0x38),
        '!' => (LEFT_SHIFT, 0x1e),
        '@' => (LEFT_SHIFT, 0x1f),
        '#' => (LEFT_SHIFT, 0x20),
        '$' => (LEFT_SHIFT, 0x21),
        '%' => (LEFT_SHIFT, 0x22),
        '^' => (LEFT_SHIFT, 0x23),
        '&' => (LEFT_SHIFT, 0x24),
        '*' => (LEFT_SHIFT, 0x25),
        '(' => (LEFT_SHIFT, 0x26),
        ')' => (LEFT_SHIFT, 0x27),
        _ => return None,
    };
    Some(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn converts_ascii_boundaries_and_modifiers() {
        assert_eq!(us_ascii_usage('a'), Some((0, 0x04)));
        assert_eq!(us_ascii_usage('z'), Some((0, 0x1d)));
        assert_eq!(us_ascii_usage('A'), Some((LEFT_SHIFT, 0x04)));
        assert_eq!(us_ascii_usage('!'), Some((LEFT_SHIFT, 0x1e)));
        assert_eq!(us_ascii_usage('?'), Some((LEFT_SHIFT, 0x38)));
        assert_eq!(us_ascii_usage('\n'), Some((0, 0x28)));
        assert_eq!(us_ascii_usage('\u{7f}'), None);
        assert_eq!(us_ascii_usage('é'), None);
    }

    #[test]
    fn macro_releases_every_character() {
        let steps = text_to_macro("Aa").expect("text should convert");
        assert_eq!(steps.len(), 4);
        assert_eq!(steps[0].modifier, LEFT_SHIFT);
        assert_eq!(steps[1], MacroStep::release());
        assert_eq!(steps[2].modifier, 0);
        assert_eq!(steps[3], MacroStep::release());
    }

    #[test]
    fn collapses_crlf_to_one_enter() {
        let steps = text_to_macro("a\r\nb\r\n").expect("text should convert");
        let enter_steps = steps.iter().filter(|step| step.keys[0] == 0x28).count();
        assert_eq!(enter_steps, 2);
        assert_eq!(
            text_to_macro("\r")
                .expect("carriage return should convert")
                .len(),
            2
        );
        assert_eq!(
            text_to_macro("\n").expect("newline should convert").len(),
            2
        );
    }
}
