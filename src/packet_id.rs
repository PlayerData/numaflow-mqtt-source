use std::mem::size_of;

use numaflow::source::Offset;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct PacketID(u16);

impl TryFrom<Offset> for PacketID {
    type Error = anyhow::Error;

    fn try_from(offset: Offset) -> Result<Self, Self::Error> {
        if offset.offset.len() != size_of::<PacketID>() {
            return Err(anyhow::anyhow!(
                "Received invalid offset length from Numaflow"
            ));
        }

        let pkid_bytes: [u8; size_of::<PacketID>()] = offset.offset.try_into().unwrap();

        Ok(PacketID(u16::from_be_bytes(pkid_bytes)))
    }
}

impl From<PacketID> for Offset {
    fn from(pkid: PacketID) -> Self {
        let offset_bytes = pkid.0.to_be_bytes().to_vec();

        Offset {
            offset: offset_bytes,
            partition_id: 0,
        }
    }
}

impl From<u16> for PacketID {
    fn from(pkid: u16) -> Self {
        PacketID(pkid)
    }
}
