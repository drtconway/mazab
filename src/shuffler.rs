use rand::{rngs::StdRng, Rng, SeedableRng};

pub struct Shuffler<T, U>
where
    T: Iterator<Item = U>,
{
    n: usize,
    buffer: Vec<U>,
    rng: StdRng,
    pub src: T,
}

impl<T, U> Shuffler<T, U>
where
    T: Iterator<Item = U>,
{
    pub fn new(buffer_size: usize, seed: u64, src: T) -> Shuffler<T, U> {
        let mut res = Shuffler {
            n: buffer_size,
            buffer: Vec::new(),
            rng: StdRng::seed_from_u64(seed),
            src,
        };
        res.buffer.reserve(buffer_size);
        while res.buffer.len() < res.n {
            if let Some(item) = res.src.next() {
                res.buffer.push(item);
            } else {
                break;
            }
        }
        res
    }
}

impl<T, U> Iterator for Shuffler<T, U>
where
    T: Iterator<Item = U>,
{
    type Item = U;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut item) = self.src.next() {
            debug_assert_eq!(self.buffer.len(), self.n);
            let j = self.rng.gen_range(0..self.buffer.len());
            std::mem::swap(&mut item, &mut self.buffer[j]);
            Some(item)
        } else {
            if let Some(mut item) = self.buffer.pop() {
                if self.buffer.len() > 1 {
                    let j = self.rng.gen_range(0..self.buffer.len());
                    std::mem::swap(&mut item, &mut self.buffer[j]);
                }
                Some(item)
            } else {
                None
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let src_hint = self.src.size_hint();
        let lower = src_hint.0 + self.buffer.len();
        let upper = src_hint.1.map(|n| n + self.buffer.len());
        (lower, upper)
    }
}
