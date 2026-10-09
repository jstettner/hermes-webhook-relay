import { GranolaIngestion } from '../granola'
import { webhookHandler } from './webhook'

export const granolaHandler = webhookHandler(GranolaIngestion)
