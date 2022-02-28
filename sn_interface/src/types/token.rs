// Copyright 2022 MaidSafe.net limited.
//
// This SAFE Network Software is licensed to you under The General Public License (GPL), version 3.
// Unless required by applicable law or agreed to in writing, the SAFE Network Software distributed
// under the GPL Licence is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. Please review the Licences for the specific language governing
// permissions and limitations relating to use of the SAFE Network Software.

use super::errors::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

/// The conversion from Token to raw value
const TOKEN_TO_RAW_POWER_OF_10_CONVERSION: u32 = 9;

/// The conversion from Token to raw value
const TOKEN_TO_RAW_CONVERSION: u64 = 1_000_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
/// Structure representing a Token amount.
pub struct Token(u64);

impl Token {
    /// Type safe representation of zero Token.
    pub const fn zero() -> Self {
        Self(0)
    }

    /// New value from a number of nano tokens.
    pub const fn from_nano(value: u64) -> Self {
        Self(value)
    }

    /// Total Token expressed in number of nano tokens.
    pub const fn as_nano(self) -> u64 {
        self.0
    }

    /// Computes `self + rhs`, returning `None` if overflow occurred.
    pub fn checked_add(self, rhs: Token) -> Option<Token> {
        self.0.checked_add(rhs.0).map(Self::from_nano)
    }

    /// Computes `self - rhs`, returning `None` if overflow occurred.
    pub fn checked_sub(self, rhs: Token) -> Option<Token> {
        self.0.checked_sub(rhs.0).map(Self::from_nano)
    }
}

impl FromStr for Token {
    type Err = Error;

    fn from_str(value_str: &str) -> Result<Self> {
        let mut itr = value_str.splitn(2, '.');
        let converted_units = {
            let units = itr
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                .ok_or_else(|| Error::FailedToParse("Can't parse token units".to_string()))?;

            units
                .checked_mul(TOKEN_TO_RAW_CONVERSION)
                .ok_or(Error::ExcessiveValue)?
        };

        let remainder = {
            let remainder_str = itr.next().unwrap_or_default().trim_end_matches('0');

            if remainder_str.is_empty() {
                0
            } else {
                let parsed_remainder = remainder_str
                    .parse::<u64>()
                    .map_err(|_| Error::FailedToParse("Can't parse token remainder".to_string()))?;

                let remainder_conversion = TOKEN_TO_RAW_POWER_OF_10_CONVERSION
                    .checked_sub(remainder_str.len() as u32)
                    .ok_or(Error::LossOfPrecision)?;
                parsed_remainder * 10_u64.pow(remainder_conversion)
            }
        };

        Ok(Self::from_nano(converted_units + remainder))
    }
}

impl Display for Token {
    fn fmt(&self, formatter: &mut Formatter) -> fmt::Result {
        let unit = self.0 / TOKEN_TO_RAW_CONVERSION;
        let remainder = self.0 % TOKEN_TO_RAW_CONVERSION;
        write!(formatter, "{}.{:09}", unit, remainder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::u64;

    #[test]
    fn from_str() -> Result<()> {
        assert_eq!(Token(0), Token::from_str("0")?);
        assert_eq!(Token(0), Token::from_str("0.")?);
        assert_eq!(Token(0), Token::from_str("0.0")?);
        assert_eq!(Token(1), Token::from_str("0.000000001")?);
        assert_eq!(Token(1_000_000_000), Token::from_str("1")?);
        assert_eq!(Token(1_000_000_000), Token::from_str("1.")?);
        assert_eq!(Token(1_000_000_000), Token::from_str("1.0")?);
        assert_eq!(Token(1_000_000_001), Token::from_str("1.000000001")?);
        assert_eq!(Token(1_100_000_000), Token::from_str("1.1")?);
        assert_eq!(Token(1_100_000_001), Token::from_str("1.100000001")?);
        assert_eq!(
            Token(4_294_967_295_000_000_000),
            Token::from_str("4294967295")?
        );
        assert_eq!(
            Token(4_294_967_295_999_999_999),
            Token::from_str("4294967295.999999999")?,
        );
        assert_eq!(
            Token(4_294_967_295_999_999_999),
            Token::from_str("4294967295.9999999990000")?,
        );

        assert_eq!(
            Err(Error::FailedToParse("Can't parse token units".to_string())),
            Token::from_str("a")
        );
        assert_eq!(
            Err(Error::FailedToParse(
                "Can't parse token remainder".to_string()
            )),
            Token::from_str("0.a")
        );
        assert_eq!(
            Err(Error::FailedToParse(
                "Can't parse token remainder".to_string()
            )),
            Token::from_str("0.0.0")
        );
        assert_eq!(Err(Error::LossOfPrecision), Token::from_str("0.0000000009"));
        assert_eq!(Err(Error::ExcessiveValue), Token::from_str("18446744074"));
        Ok(())
    }

    #[test]
    fn display() {
        assert_eq!("0.000000000", format!("{}", Token(0)));
        assert_eq!("0.000000001", format!("{}", Token(1)));
        assert_eq!("0.000000010", format!("{}", Token(10)));
        assert_eq!("1.000000000", format!("{}", Token(1_000_000_000)));
        assert_eq!("1.000000001", format!("{}", Token(1_000_000_001)));
        assert_eq!(
            "4294967295.000000000",
            format!("{}", Token(4_294_967_295_000_000_000))
        );
    }

    #[test]
    fn checked_add_sub() {
        assert_eq!(Some(Token(3)), Token(1).checked_add(Token(2)));
        assert_eq!(None, Token(u64::MAX).checked_add(Token(1)));
        assert_eq!(None, Token(u64::MAX).checked_add(Token(u64::MAX)));

        assert_eq!(Some(Token(0)), Token(u64::MAX).checked_sub(Token(u64::MAX)));
        assert_eq!(None, Token(0).checked_sub(Token(u64::MAX)));
        assert_eq!(None, Token(10).checked_sub(Token(11)));
    }
}
