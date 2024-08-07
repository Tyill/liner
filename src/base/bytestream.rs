use std::io::{Read, Result, Write};

mod bytestream{

#[derive(Copy, Clone)]
pub enum ByteOrder {
    BigEndian,
    LittleEndian,
}

pub trait StreamReader: Sized {
    fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self>;
}

pub trait StreamWriter: Sized {
    fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()>;
}

//////////////////////////////////////////////////////////////////////////////
// Boolean

impl StreamReader for bool {
    fn read_from<R: Read>(buffer: &mut R, _order: ByteOrder) -> Result<Self> {
        Ok(buffer.read_u8()? == 1)
    }
}

impl StreamWriter for bool {
    fn write_to<W: Write>(&self, buffer: &mut W, _order: ByteOrder) -> Result<()> {
        buffer.write_u8(if *self { 1 } else { 0 })?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////////////////////////
// Unsigned integers

impl StreamReader for u64 {
    fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self> {
        match order {
            ByteOrder::BigEndian => Ok(buffer.read_u64::<BigEndian>()?),
            ByteOrder::LittleEndian => Ok(buffer.read_u64::<LittleEndian>()?),
        }
    }
}

impl StreamWriter for u64 {
    fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()> {
        match order {
            ByteOrder::BigEndian => buffer.write_u64::<BigEndian>(*self)?,
            ByteOrder::LittleEndian => buffer.write_u64::<LittleEndian>(*self)?,
        }
        Ok(())
    }
}

impl StreamReader for u32 {
    fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self> {
        match order {
            ByteOrder::BigEndian => Ok(buffer.read_u32::<BigEndian>()?),
            ByteOrder::LittleEndian => Ok(buffer.read_u32::<LittleEndian>()?),
        }
    }
}

impl StreamWriter for u32 {
    fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()> {
        match order {
            ByteOrder::BigEndian => buffer.write_u32::<BigEndian>(*self)?,
            ByteOrder::LittleEndian => buffer.write_u32::<LittleEndian>(*self)?,
        }
        Ok(())
    }
}

impl StreamReader for u16 {
    fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self> {
        match order {
            ByteOrder::BigEndian => Ok(buffer.read_u16::<BigEndian>()?),
            ByteOrder::LittleEndian => Ok(buffer.read_u16::<LittleEndian>()?),
        }
    }
}

impl StreamWriter for u16 {
    fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()> {
        match order {
            ByteOrder::BigEndian => buffer.write_u16::<BigEndian>(*self)?,
            ByteOrder::LittleEndian => buffer.write_u16::<LittleEndian>(*self)?,
        }
        Ok(())
    }
}

impl StreamReader for u8 {
    fn read_from<R: Read>(buffer: &mut R, _order: ByteOrder) -> Result<Self> {
        Ok(buffer.read_u8()?)
    }
}

impl StreamWriter for u8 {
    fn write_to<W: Write>(&self, buffer: &mut W, _order: ByteOrder) -> Result<()> {
        buffer.write_u8(*self)?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////////////////////////
// Signed integers

impl StreamReader for i64 {
    fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self> {
        match order {
            ByteOrder::BigEndian => Ok(buffer.read_i64::<BigEndian>()?),
            ByteOrder::LittleEndian => Ok(buffer.read_i64::<LittleEndian>()?),
        }
    }
}

impl StreamWriter for i64 {
    fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()> {
        match order {
            ByteOrder::BigEndian => buffer.write_i64::<BigEndian>(*self)?,
            ByteOrder::LittleEndian => buffer.write_i64::<LittleEndian>(*self)?,
        }
        Ok(())
    }
}

impl StreamReader for i32 {
    fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self> {
        match order {
            ByteOrder::BigEndian => Ok(buffer.read_i32::<BigEndian>()?),
            ByteOrder::LittleEndian => Ok(buffer.read_i32::<LittleEndian>()?),
        }
    }
}

impl StreamWriter for i32 {
    fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()> {
        match order {
            ByteOrder::BigEndian => buffer.write_i32::<BigEndian>(*self)?,
            ByteOrder::LittleEndian => buffer.write_i32::<LittleEndian>(*self)?,
        }
        Ok(())
    }
}

impl StreamReader for i16 {
    fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self> {
        match order {
            ByteOrder::BigEndian => Ok(buffer.read_i16::<BigEndian>()?),
            ByteOrder::LittleEndian => Ok(buffer.read_i16::<LittleEndian>()?),
        }
    }
}

impl StreamWriter for i16 {
    fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()> {
        match order {
            ByteOrder::BigEndian => buffer.write_i16::<BigEndian>(*self)?,
            ByteOrder::LittleEndian => buffer.write_i16::<LittleEndian>(*self)?,
        }
        Ok(())
    }
}

impl StreamReader for i8 {
    fn read_from<R: Read>(buffer: &mut R, _order: ByteOrder) -> Result<Self> {
        Ok(buffer.read_i8()?)
    }
}

impl StreamWriter for i8 {
    fn write_to<W: Write>(&self, buffer: &mut W, _order: ByteOrder) -> Result<()> {
        buffer.write_i8(*self)?;
        Ok(())
    }
}

//////////////////////////////////////////////////////////////////////////////
// Tests

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{StreamReader, StreamWriter};
    use std::{fmt::Debug, io::Cursor};

    #[derive(Debug, PartialEq)]
    pub struct Foo {
        pub foo: u32,
        pub bar: u16,
    }

    impl StreamReader for Foo {
        fn read_from<R: Read>(buffer: &mut R, order: ByteOrder) -> Result<Self> {
            Ok(Self {
                foo: u32::read_from(buffer, order)?,
                bar: u16::read_from(buffer, order)?,
            })
        }
    }

    impl StreamWriter for Foo {
        fn write_to<W: Write>(&self, buffer: &mut W, order: ByteOrder) -> Result<()> {
            self.foo.write_to(buffer, order)?;
            self.bar.write_to(buffer, order)?;
            Ok(())
        }
    }

    fn serialize<T: StreamReader + StreamWriter>(input: T, order: ByteOrder) -> T {
        let mut buffer = Vec::<u8>::new();
        input.write_to(&mut buffer, order).unwrap();

        let mut cursor = Cursor::new(buffer);
        T::read_from(&mut cursor, order).unwrap()
    }

    mod std_types {
        use super::*;
        use test_case::test_case;

        #[test_case(u8::max_value(), ByteOrder::BigEndian ; "u8 big endian")]
        #[test_case(u8::max_value(), ByteOrder::LittleEndian ; "u8 little endian")]
        #[test_case(u16::max_value(), ByteOrder::BigEndian ; "u16 big endian")]
        #[test_case(u16::max_value(), ByteOrder::LittleEndian ; "u16 little endian")]
        #[test_case(u32::max_value(), ByteOrder::BigEndian ; "u32 big endian")]
        #[test_case(u32::max_value(), ByteOrder::LittleEndian ; "u32 little endian")]
        #[test_case(u64::max_value(), ByteOrder::BigEndian ; "u64 big endian")]
        #[test_case(u64::max_value(), ByteOrder::LittleEndian ; "u64 little endian")]
        #[test_case(i8::max_value(), ByteOrder::BigEndian ; "i8 big endian")]
        #[test_case(i8::max_value(), ByteOrder::LittleEndian ; "i8 little endian")]
        #[test_case(i16::max_value(), ByteOrder::BigEndian ; "i16 big endian")]
        #[test_case(i16::max_value(), ByteOrder::LittleEndian ; "i16 little endian")]
        #[test_case(i32::max_value(), ByteOrder::BigEndian ; "i32 big endian")]
        #[test_case(i32::max_value(), ByteOrder::LittleEndian ; "i32 little endian")]
        #[test_case(i64::max_value(), ByteOrder::BigEndian ; "i64 big endian")]
        #[test_case(i64::max_value(), ByteOrder::LittleEndian ; "i64 little endian")]
        pub fn max_value<T: StreamReader + StreamWriter + PartialEq + Debug + Copy + Clone>(
            input: T,
            order: ByteOrder,
        ) {
            // Given, When
            let result = serialize(input, order);
            // Then
            assert_eq!(input, result);
        }

        #[test_case(u8::min_value(), ByteOrder::BigEndian ; "u8 big endian")]
        #[test_case(u8::min_value(), ByteOrder::LittleEndian ; "u8 little endian")]
        #[test_case(u16::min_value(), ByteOrder::BigEndian ; "u16 big endian")]
        #[test_case(u16::min_value(), ByteOrder::LittleEndian ; "u16 little endian")]
        #[test_case(u32::min_value(), ByteOrder::BigEndian ; "u32 big endian")]
        #[test_case(u32::min_value(), ByteOrder::LittleEndian ; "u32 little endian")]
        #[test_case(u64::min_value(), ByteOrder::BigEndian ; "u64 big endian")]
        #[test_case(u64::min_value(), ByteOrder::LittleEndian ; "u64 little endian")]
        #[test_case(i8::min_value(), ByteOrder::BigEndian ; "i8 big endian")]
        #[test_case(i8::min_value(), ByteOrder::LittleEndian ; "i8 little endian")]
        #[test_case(i16::min_value(), ByteOrder::BigEndian ; "i16 big endian")]
        #[test_case(i16::min_value(), ByteOrder::LittleEndian ; "i16 little endian")]
        #[test_case(i32::min_value(), ByteOrder::BigEndian ; "i32 big endian")]
        #[test_case(i32::min_value(), ByteOrder::LittleEndian ; "i32 little endian")]
        #[test_case(i64::min_value(), ByteOrder::BigEndian ; "i64 big endian")]
        #[test_case(i64::min_value(), ByteOrder::LittleEndian ; "i64 little endian")]
        pub fn min_value<T: StreamReader + StreamWriter + PartialEq + Debug + Copy + Clone>(
            input: T,
            order: ByteOrder,
        ) {
            // Given, When
            let result = serialize(input, order);
            // Then
            assert_eq!(input, result);
        }

        // Endianess shouldn't matter for booleans, since it's only a byte,
        // but we still want a test for it.
        #[test_case(true, ByteOrder::BigEndian ; "true big endian")]
        #[test_case(true, ByteOrder::LittleEndian ; "true little endian")]
        #[test_case(false, ByteOrder::BigEndian ; "false big endian")]
        #[test_case(false, ByteOrder::LittleEndian ; "false little endian")]
        pub fn boolean(input: bool, order: ByteOrder) {
            // Given, When
            let result = serialize(input, order);
            // Then
            assert_eq!(input, result);
        }
    }
}
}