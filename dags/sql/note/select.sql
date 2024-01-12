select * from note a
inner join NOTE_NLP b on a.note_id = b.note_id
order by a.note_id 